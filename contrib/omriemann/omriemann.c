#include "rsyslog.h"
#include "module-template.h"
#include "errmsg.h"
#include "conf.h"
#include "msg.h"
#include <json.h>

#include <riemann/riemann-client.h>
#include <riemann/simple.h>


MODULE_TYPE_OUTPUT
MODULE_TYPE_NOKEEP
MODULE_CNFNAME("omriemann")

DEF_OMOD_STATIC_DATA
DEFobjCurrIf(errmsg)

#define OMRIEMANN_FIELD_HOST 0
#define OMRIEMANN_FIELD_SERVICE 1
#define OMRIEMANN_FIELD_METRIC 2
#define FIELD_COUNT 13
#define MAX_TAG_COUNT 16

typedef struct _instanceData {
  uchar *server;
  int port;

  int mode;

  uchar *host;
  uchar *time;
  uchar *service;
  uchar *metric;
  uchar *description;

  sbool includeall;

  msgPropDescr_t *propHost;
  msgPropDescr_t *propTime;
  msgPropDescr_t *propService;
  msgPropDescr_t *propMetric; 
  msgPropDescr_t *propDescription; 
  msgPropDescr_t *propSubtree;

  char *prefix; 
  int prefixLen;

} instanceData;

typedef struct wrkrInstanceData {
	instanceData *pData; 
	riemann_client_t *client; 
  int count;
} wrkrInstanceData_t;

static pthread_mutex_t mutDoAct = PTHREAD_MUTEX_INITIALIZER;

static struct cnfparamdescr actpdescr[] = {
  { "server", eCmdHdlrGetWord, 0 },
  { "subtree", eCmdHdlrGetWord, 0 },
  { "serverport", eCmdHdlrInt, 0 },
  { "service", eCmdHdlrGetWord, 0 },
  { "description", eCmdHdlrGetWord, 0 },
  { "metric", eCmdHdlrGetWord, 0 },
  { "host", eCmdHdlrGetWord, 0 },
  { "time", eCmdHdlrGetWord, 0 },
  { "mode", eCmdHdlrGetWord, 0 },
  { "prefix", eCmdHdlrGetWord, 0 },
  { "includeall", eCmdHdlrBinary, 0 },
};

static struct cnfparamblk actpblk = {
	CNFPARAMBLK_VERSION,
	sizeof(actpdescr)/sizeof(struct cnfparamdescr),
	actpdescr
};

static void closeRiemannClient(wrkrInstanceData_t *pWrkrData)
{
    if(pWrkrData->client != NULL) {
        riemann_client_free(pWrkrData->client);
        pWrkrData->client = NULL;
    }
}

BEGINcreateInstance
CODESTARTcreateInstance
ENDcreateInstance

BEGINcreateWrkrInstance
CODESTARTcreateWrkrInstance
	pWrkrData->client = NULL; /* Connect later */
ENDcreateWrkrInstance

BEGINfreeInstance
CODESTARTfreeInstance
	if (pData->server != NULL) {
		free(pData->server);
	}
ENDfreeInstance

BEGINfreeWrkrInstance
CODESTARTfreeWrkrInstance
	closeRiemannClient(pWrkrData);
ENDfreeWrkrInstance

BEGINdbgPrintInstInfo
CODESTARTdbgPrintInstInfo
	/* nothing special here */
ENDdbgPrintInstInfo



rsRetVal ensureRiemannConnectionIsOpen(wrkrInstanceData_t *pWrkrData)
{
  DEFiRet;
  const char *server;
  server = (const char *)pWrkrData->pData->server;

  if (NULL != pWrkrData->client) {
   RETiRet;
  }

  pWrkrData->client = riemann_client_create(RIEMANN_CLIENT_TCP, server, pWrkrData->pData->port);
  if (NULL == pWrkrData->client) {
    dbgprintf("omriemann: can't connect to Riemann at %s %d", server, 5555);
    ABORT_FINALIZE(RS_RET_SUSPENDED);
  }
    
  finalize_it:
    RETiRet;
}

/*********************************************************
 *
 * The eventlist is our internal datastructure.
 * For each event, we store an array of FIELD_COUNT void pointers.
 * We use the riemann_event_field_t enum as an index into
 *   the array.
 *
 * For some messages, eg impstats, we will generate a set of
 * events, so we can treat events as a linked list.
 *
 * ********************************************************/

typedef struct eventlist {
  void **fields;
  struct eventlist *next;
} eventlist_t;

static eventlist_t*
eventlist_new() {
   eventlist_t *result;
   void * fields;

   result = malloc(sizeof(eventlist_t));
   fields = calloc(FIELD_COUNT, sizeof(void *));
   result->fields = fields;
   result->next = NULL;

   return result;
}

static void 
eventlist_free(eventlist_t *list)
{
   eventlist_t *tmp;
   int i = 0;
   char** tags = (char**)list->fields[RIEMANN_EVENT_FIELD_TAGS];

   if(NULL != tags)
   {
        while (NULL != tags[i]) {
            free(tags[i++]);
        }
   }
   while (list != NULL)
   {
     for(i =0; i< FIELD_COUNT; i++) {
        if (list->fields[i] != NULL){
        
           free(list->fields[i]);
        }
     }

      free(list->fields);
      tmp = list;
      list = list->next;
      free(tmp);
   }
}

/*********************************************
 *
 * Various odds and sods for setting fields.
 *
 * *******************************************/


/* We allow literals or property names in our config.
 * This function sets a field with a string value to 
 * either the literal value of the config key, or to 
 * the value of the resolved msgPropDescr_t.
 * 
 * We always call this function for every configurable
 * field so that we can apply  defaults, which is why 
 * we check to make certain the field hasn't already 
 * been set.
 * * */


static void 
readConfigValue (char **target, smsg_t *msg, uchar *cfgValue, msgPropDescr_t *resolvedProperty) {
        uchar *propValue; 
        short unsigned mustFree = 0; 
        rs_size_t propLen;
        propValue = NULL;

        // already set? Nothing to do.
        if(NULL != *target) {
          return;
        }
        // if we have a resolved property for this field, use the property value
        if (NULL != resolvedProperty)
        {
           propValue = MsgGetProp(msg, NULL, resolvedProperty, &propLen, &mustFree, NULL);
           if( NULL != propValue)
              *(char**)target = strdup((char *)propValue);

        // Otherwise, if the field has a value, treat it as a literal.
        } else if(NULL != cfgValue) {
          *(char**)target = strdup((char *)cfgValue);
        }
        if(mustFree)
            free(propValue);
}

static void 
setFieldFromConfig(void **event, smsg_t *msg,
                riemann_event_field_t field, uchar *cfgValue, 
                msgPropDescr_t *resolvedProperty) {
    
        readConfigValue(&(event[field]), msg, cfgValue, resolvedProperty);
}

/* Parse a string and set either the metric_s64 or metric_d fields in the event*/
static unsigned short 
setMetricFromString(const char* value, eventlist_t *event)
{
    double dValue;
    int64_t iValue;
    char* errPtr;

    iValue = strtol((const char *)value, &errPtr, 10);

    // if we parsed to the end of the string, then we have a simple integer.
    if ('\0' == *errPtr) {
      event->fields[RIEMANN_EVENT_FIELD_METRIC_S64] = calloc(1, sizeof(int64_t));
      *((int64_t *)event->fields[RIEMANN_EVENT_FIELD_METRIC_S64]) = iValue;
      return 1;
    }

    // we were given no characters at all, so we won't set a metric field.
    if(errPtr == value)
      return 0;

    // if we failed on a decimal point, let's try parsing this as a decimal.
    if (*errPtr == '.')
    {
      errPtr = NULL;
      dValue = strtod(value, &errPtr);
    }

    // if we reached the end of the string, we have a decimal.
    if('\0' == *errPtr) {
       event->fields[RIEMANN_EVENT_FIELD_METRIC_D] = calloc(1, sizeof(double));
       *((double*)event->fields[RIEMANN_EVENT_FIELD_METRIC_D]) = dValue;
       return 1;
    }

    // otherwise we've got a non-numeric string.
    return 0;
}

/* Parses a json rValue and sets the metric_s64 or metric_d fields */
static unsigned short 
setMetricFromJsonValue(struct json_object *json, eventlist_t *event)
{
        json_type type;

        if (NULL == json)
           return 0;
        
        type = json_object_get_type(json);

        switch(type) {
          case json_type_null:
          case json_type_array:
          case json_type_object:
            // honestly can't give you a metric in this situation.
            // sorry.
            return 0;
          case json_type_boolean:
          case json_type_int:
            // we'll treat bool as a special case of integer.
            event->fields[RIEMANN_EVENT_FIELD_METRIC_S64] = calloc(1, sizeof(int64_t));
            *((int64_t *)event->fields[RIEMANN_EVENT_FIELD_METRIC_S64]) = json_object_get_int64(json);
            return 1;
          case json_type_double:
            // double is simple.
            event->fields[RIEMANN_EVENT_FIELD_METRIC_D] = calloc(1, sizeof(double));
            *((double *)event->fields[RIEMANN_EVENT_FIELD_METRIC_D]) = json_object_get_double(json);
            return 1;
          case json_type_string:
            // if we were given a string, we'll try and parse it.
            return setMetricFromString(json_object_get_string(json), event);
       }
       return 0;
}

/* Take the config setting for `metric` and parse it to set a metric value*/
static void
setMetricFromConfig(eventlist_t *event, smsg_t *msg, instanceData *cfg)
{
    // If the metric has already been set, then we won't override it.
    if( NULL != event->fields[RIEMANN_EVENT_FIELD_METRIC_S64] || 
        NULL != event->fields[RIEMANN_EVENT_FIELD_METRIC_D]) 
       return;

    int localRet;
    uchar* value;
    rs_size_t valueLen;
    unsigned short mustFree;

    struct json_object *json;

    // If there is no resolved property for metric then we'll treat metric as a literal string
    // This is the default case since we set metric to "1" if it isn't provided by a user.
    if (NULL == cfg->propMetric) {
       setMetricFromString((char *)cfg->metric, event);
    }

    else {
        // we resolved a property earlier, so now we need to get it.
        switch(cfg->propMetric->id) {
          // if we were given a JSON value, we should parse the metric from json
          case PROP_CEE:
          case PROP_CEE_ALL_JSON:
          case PROP_CEE_ALL_JSON_PLAIN:
            
            localRet = msgGetJSONPropJSON(msg, cfg->propMetric, &json);
            if (localRet == RS_RET_OK)
              setMetricFromJsonValue(json, event);
            break;
          // otherwise we'll parse it from the string.
          default:
             value = MsgGetProp(msg, NULL, cfg->propMetric, &valueLen, &mustFree, NULL);
             setMetricFromString((char *)value, event);
             if(mustFree) 
               free(value);
        }
    }
}


/* This is just a big list of fields that we might have set in config. We use these as defaults if 
 * no value is provided in the incoming message.
 * We treat some fields as mandatory, and always set a default for them. These are
 *   - host
 *   - service
 *   - time
 *   - metric
 * None of these are _actually_ required by riemann, but setting them by default means that the 
 * resulting message is vaguely sensible.
 */

static void 
setFieldsFromConfig(eventlist_t *event, smsg_t *msg, instanceData *cfg)
{
    setFieldFromConfig(event->fields, msg, RIEMANN_EVENT_FIELD_HOST, cfg->host, cfg->propHost);
    setFieldFromConfig(event->fields, msg, RIEMANN_EVENT_FIELD_SERVICE, cfg->service, cfg->propService);
    setFieldFromConfig(event->fields, msg, RIEMANN_EVENT_FIELD_DESCRIPTION, cfg->description, cfg->propDescription);
    setMetricFromConfig(event, msg, cfg);
}


/* If we receive an impstats message, we use this function to set the service according to the `name` field and they key of the metric.
 * eg: given the json data {name="action 0", processed=10, failed=2} we will send two events with 
 * the service names "action 0/processed" and "action 0/failed"
 *
 * This function also handles prefixing service names, eg. action(type="omriemann" prefix="foo") will yield metrics with service names
 * `foo/$programname`
 */ 
static void setServiceName(void **fields, const char* prefix, size_t prefixLen, const char* instanceName, size_t instanceNameLen, const char* metricName) {
    
    int serviceNameLen = 0;
    int offset = 0;
    size_t metricNameLen = 0;

    if (instanceNameLen == 0 && prefixLen == 0) {
       fields[RIEMANN_EVENT_FIELD_SERVICE] = strdup(metricName);
    }
    else
    {
        metricNameLen = strlen(metricName);

        char* serviceName = malloc((prefixLen + instanceNameLen + metricNameLen) * sizeof(char));

        if (prefixLen > 0) {
            strcpy(serviceName, prefix);
            serviceName[prefixLen] = '/';
            offset = prefixLen + 1;
        }
        if (instanceNameLen > 0) {
            strcpy(serviceName + offset, instanceName);
            serviceName[offset + instanceNameLen] = '/'; 
            offset += (1 + instanceNameLen);
        }
        if (metricNameLen > 0) {
                strcpy(serviceName + offset, metricName);
        }
        fields[RIEMANN_EVENT_FIELD_SERVICE] = serviceName;
    } 
}

static int buildSingleEvent(instanceData *cfg, eventlist_t *next, json_object *root)
{
    struct json_object_iterator it;
    struct json_object_iterator itEnd;
    struct json_object *val;
    struct json_object *tag;
    enum json_type type;
    const char* name = NULL;
    char ** tags;
    int hasValues = 0;
    int i = 0;
    int len = 0;

    it = json_object_iter_begin(root);
    itEnd = json_object_iter_end(root);

    while( !json_object_iter_equal(&it, &itEnd) )
    {
       val = json_object_iter_peek_value(&it);
       type = json_object_get_type(val);
       name = json_object_iter_peek_name(&it);

       if(strcmp(name, "service") == 0 && type == json_type_string) {
            setServiceName(next->fields, cfg->prefix, cfg->prefixLen, NULL, 0, json_object_get_string(val));
            hasValues = 1;
       }
       else if(!strcmp(name, "metric")) {
            setMetricFromJsonValue(val, next);
            hasValues = 1;
       }
       else if (!strcmp(name, "state")) {
            next->fields[RIEMANN_EVENT_FIELD_STATE] = strdup(json_object_get_string(val));
            hasValues = 1;
       }
       else if (!strcmp(name, "description")) {
            next->fields[RIEMANN_EVENT_FIELD_DESCRIPTION] = strdup(json_object_get_string(val));
            hasValues = 1;
       }
       else if (!strcmp(name, "host")) {
            next->fields[RIEMANN_EVENT_FIELD_HOST] = strdup(json_object_get_string(val));
            hasValues = 1;
       }
       else if (!strcmp(name, "ttl") && (type == json_type_double)) {
            next->fields[RIEMANN_EVENT_FIELD_TTL] = calloc(1, sizeof(float));
            *((float *)next->fields[RIEMANN_EVENT_FIELD_TTL]) = (float)json_object_get_double(val);
       }
       else if (!strcmp(name, "ttl") && (type == json_type_int)) {
            next->fields[RIEMANN_EVENT_FIELD_TTL] = calloc(1, sizeof(float));
            *((float *)next->fields[RIEMANN_EVENT_FIELD_TTL]) = (float)json_object_get_int(val);
       }
       else if (!strcmp(name, "tags") && type == json_type_array) {
            tags = calloc(MAX_TAG_COUNT, sizeof(char*));
            len = json_object_array_length(val);
            next->fields[RIEMANN_EVENT_FIELD_TAGS] = tags;

            for(i = 0; i < len; i++)
            {
                tag = json_object_array_get_idx(val, i);
                type = json_object_get_type(tag);
                if (type == json_type_string) {
                   tags[i] = strdup(json_object_get_string(tag)); 
                } 
            }
       }
       else if (!strcmp(name, "attributes") && type == json_type_object) {
           struct json_object_iterator attribsIt = json_object_iter_begin(val);;
           struct json_object_iterator attribsItEnd = json_object_iter_end(val);
           riemann_attribute_t ** attributes = calloc(MAX_TAG_COUNT, sizeof(riemann_attribute_t*)); 

           i = 0;

           while (!json_object_iter_equal(&attribsIt, &attribsItEnd))
           {
               val = json_object_iter_peek_value(&attribsIt);
               name = json_object_iter_peek_name(&attribsIt);

               attributes[i ++] = riemann_attribute_create(name, json_object_get_string(val));
               json_object_iter_next(&attribsIt);
           }

           next->fields[RIEMANN_EVENT_FIELD_ATTRIBUTES] = attributes;
       }
       json_object_iter_next(&it);
    }

    if(NULL != val) {
        free(val);
    }

    return hasValues;
}


/* This is the function responsible for mapping a message to an eventlist_t. */
static eventlist_t *
makeEventsFromMessage(smsg_t *msg, instanceData *cfg) 
{
    eventlist_t * list = NULL; 
    eventlist_t *next = NULL;
    struct json_object *json = NULL;
    struct json_object_iterator it;
    struct json_object_iterator itEnd;
    struct json_object_iterator valuesIt;
    struct json_object_iterator valuesItEnd;
    struct json_object *val;
    enum json_type type;
    char* name = NULL;
    const char* instanceName = NULL;
    int instanceNameLen = 0;
    int err;
    unsigned short hasValues;

    next = eventlist_new();
    hasValues = 0;

    // This is the simple case. If we have no json subtree
    // then we're only sending a single event, based on the fields
    // that are defined in the config.
    if (NULL == cfg->propSubtree)
    {
        readConfigValue(&name, msg, 1, cfg->service, cfg->propService); 
        setServiceName(next->fields, cfg->prefix, cfg->prefixLen, instanceName, instanceNameLen, name);
        setFieldsFromConfig(next, msg, cfg);
        return next;
    }

    // If we have a subtree configured, but we're unable to parse it
    // for whatever reason, we'll fall back to just sending default fields.
    err = msgGetJSONPropJSON(msg, cfg->propSubtree, &json);
    if (NULL == json || err != RS_RET_OK) 
    {
        list = eventlist_new();
        readConfigValue(&name, msg, 1, cfg->service, cfg->propService); 
        setServiceName(next->fields, cfg->prefix, cfg->prefixLen, instanceName, instanceNameLen, name);
        setFieldsFromConfig(list, msg, cfg);
        return list;
    }

    
    // If we're in single-event mode then try to pase the subtree as
    // a single event.
    if(cfg->mode == 0)
    {

        if(buildSingleEvent(cfg, next, json)) 
        {
          next->next = list;
          list = next;
        } 
        else if (0 == hasValues) {
          eventlist_free(next);
        }

        setFieldsFromConfig(list, msg, cfg);
        free(json);
    
        return list;
    }

    // If we have found a json subtree, we'll check for a name property
    // and use it to set up the service names.
    json_object_object_get_ex(json, "name", &val);
    if(NULL != val) {
      type = json_object_get_type(val);
      if (type == json_type_string) {
         instanceName = json_object_get_string(val);
         instanceNameLen = strlen(instanceName);
      }
    }

    // For each key/value in the json subtree we'll
    // treat the key as a service name and try to parse
    // the value as a metric.
    it = json_object_iter_begin(json);
    itEnd = json_object_iter_end(json);

    while( !json_object_iter_equal(&it, &itEnd) )
    {
       hasValues = 0;
       val = json_object_iter_peek_value(&it);
       type = json_object_get_type(val);
       name = json_object_iter_peek_name(&it);
  
       if(strcmp(name, "values") == 0 && type == json_type_object) 
       {
          valuesIt = json_object_iter_begin(val);
          valuesItEnd = json_object_iter_end(val);

          while(! json_object_iter_equal(&valuesIt, &valuesItEnd))
          {
            name = json_object_iter_peek_name(&valuesIt);
            val = json_object_iter_peek_value(&valuesIt);
            if (setMetricFromJsonValue(val, next)) {
                  setServiceName(next->fields, cfg->prefix, cfg->prefixLen, instanceName, instanceNameLen, name);
                  setFieldsFromConfig(next, msg, cfg);
                  next->next = list;
                  list = next;
                  hasValues = 1;
                  next = eventlist_new();
            }
            json_object_iter_next(&valuesIt);
          }
       }
       else if (setMetricFromJsonValue(val, next)) {
          setServiceName(next->fields, cfg->prefix, cfg->prefixLen, instanceName, instanceNameLen, name);
          setFieldsFromConfig(next, msg, cfg);
          next->next = list;
          list = next;
          hasValues = 1;
          next = eventlist_new();
       }
       json_object_iter_next(&it);
    }
    if( NULL != val)
       free(val);
    free(json);
    if (0 == hasValues)
       eventlist_free(next);
    return list;
}

static void copyStringField(eventlist_t *src, riemann_event_t *event, riemann_event_field_t field)
{
    if( NULL != src->fields[field] )
     {
        riemann_event_set(event, field, src->fields[field], RIEMANN_EVENT_FIELD_NONE);
     }
}

static void copyIntField(eventlist_t *src, riemann_event_t *event, riemann_event_field_t field)
{
    if( NULL != src->fields[field] )
      riemann_event_set(event, field, *((int64_t*)src->fields[field]), RIEMANN_EVENT_FIELD_NONE);
}

static void copyDoubleField(eventlist_t *src, riemann_event_t *event, riemann_event_field_t field)
{
    if( NULL != src->fields[field] )
      riemann_event_set(event, field, *((double*)src->fields[field]), RIEMANN_EVENT_FIELD_NONE);
}

static void copyFloatField(eventlist_t *src, riemann_event_t *event, riemann_event_field_t field)
{
    if( NULL != src->fields[field] )
      riemann_event_set(event, field, *((float*)src->fields[field]), RIEMANN_EVENT_FIELD_NONE);
}

static riemann_message_t*
serializeEvents(eventlist_t *root)
{
    riemann_message_t *msg;
    int num_events = 0;
    int i = 0, j = 0;
    char ** tags;
    riemann_attribute_t **attribs;
    riemann_event_t **events;
    eventlist_t *current;

    current = root;
    // count our events
    while(current != NULL) {
      num_events ++;
      current = current->next;
    }

    events = calloc(num_events, sizeof(riemann_event_t*));

    current = root;

    while( current != NULL) {
       events[i] = riemann_event_new();
       copyStringField(current, events[i], RIEMANN_EVENT_FIELD_HOST);
       copyStringField(current, events[i], RIEMANN_EVENT_FIELD_SERVICE);
       copyStringField(current, events[i], RIEMANN_EVENT_FIELD_STATE);
       copyStringField(current, events[i], RIEMANN_EVENT_FIELD_DESCRIPTION);
       copyDoubleField(current, events[i], RIEMANN_EVENT_FIELD_METRIC_D);
       copyFloatField(current, events[i], RIEMANN_EVENT_FIELD_TTL);
       copyIntField(current, events[i], RIEMANN_EVENT_FIELD_METRIC_S64);

       if(NULL != current->fields[RIEMANN_EVENT_FIELD_TAGS]) {
           tags = (char**)current->fields[RIEMANN_EVENT_FIELD_TAGS];

           while (NULL != tags[j]) {
               riemann_event_tag_add(events[i], tags[j++]);
           }

       }

       if(NULL != current->fields[RIEMANN_EVENT_FIELD_ATTRIBUTES]) {
          j = 0;
          attribs = (riemann_attribute_t **)current->fields[RIEMANN_EVENT_FIELD_ATTRIBUTES];
          while (NULL != attribs[j]) {
                riemann_event_attribute_add(events[i], attribs[j++]);
          }
       }

       i ++;
       current = current->next;
    }

    msg = riemann_message_new();
    riemann_message_append_events_n(msg, num_events, events);
    return msg;
}


rsRetVal enqueueRiemannMetric(smsg_t *pMsg, wrkrInstanceData_t *pWrkrData) {
    DEFiRet;
    instanceData *cfg;
    riemann_message_t *riemannMessage;
    eventlist_t *events;
    cfg = pWrkrData->pData;

    events = makeEventsFromMessage(pMsg, cfg);
    if( NULL == events)
    {
       ABORT_FINALIZE(RS_RET_OK);
    }
    riemannMessage = serializeEvents(events);
    eventlist_free(events);

    CHKiRet(ensureRiemannConnectionIsOpen(pWrkrData));
    riemann_client_send_message_oneshot(pWrkrData->client, riemannMessage);
    finalize_it:
      RETiRet;    
}



BEGINtryResume
CODESTARTtryResume
	if(pWrkrData->client == NULL)
		iRet = ensureRiemannConnectionIsOpen(pWrkrData);
ENDtryResume

BEGINdoAction_NoStrings
CODESTARTdoAction
  smsg_t **msg;
  pthread_mutex_lock(&mutDoAct);
  msg = (smsg_t**)pMsgData;
  CHKiRet(enqueueRiemannMetric(*msg, pWrkrData));
  finalize_it:
	pthread_mutex_unlock(&mutDoAct);
ENDdoAction

BEGINisCompatibleWithFeature
CODESTARTisCompatibleWithFeature
	if(eFeat == sFEATURERepeatedMsgReduction)
		iRet = RS_RET_OK;
ENDisCompatibleWithFeature

static msgPropDescr_t *
getPropertyDescriptor(uchar *name)
{
    
    propid_t prop_id;
    msgPropDescr_t *prop = NULL;

    if (name == NULL) {
        return NULL;
    }
    
    propNameToID(name, &prop_id);

    if (prop_id != PROP_INVALID) 
    {
       prop = calloc(1, sizeof(msgPropDescr_t));
       msgPropDescrFill(prop, name, strlen((const char *)name));
    }

    return prop;
}

static void
setInstParamDefaults(instanceData *pData)
{
	pData->server = (uchar*) "localhost";
	pData->port = 5555;
    pData->host = (uchar*) "hostname";
    pData->time = (uchar*) "timestamp";
    pData->service = (uchar*) "programname";
    pData->metric = (uchar*) "1";
    pData->prefixLen = 0;
    pData->description = NULL;
}

BEGINnewActInst
	struct cnfparamvals *pvals;
    int i;

CODESTARTnewActInst
	if((pvals = nvlstGetParams(lst, &actpblk, NULL)) == NULL)
		ABORT_FINALIZE(RS_RET_MISSING_CNFPARAMS);

	CHKiRet(createInstance(&pData));
	setInstParamDefaults(pData);

	for(i = 0 ; i < actpblk.nParams ; ++i) {
		if(!pvals[i].bUsed)
			continue;
		if(!strcmp(actpblk.descr[i].name, "server")) {
			pData->server = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
		} else if(!strcmp(actpblk.descr[i].name, "serverport")) {
			pData->port = (int) pvals[i].val.d.n;
		} else if(!strcmp(actpblk.descr[i].name, "service")) {
			pData->service = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
		} else if(!strcmp(actpblk.descr[i].name, "host")) {
			pData->host = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
		} else if(!strcmp(actpblk.descr[i].name, "time")) {
			pData->time = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
		} else if(!strcmp(actpblk.descr[i].name, "includeall")) {
			pData->includeall = pvals[i].val.d.n;
		} else if(!strcmp(actpblk.descr[i].name, "metric")) {
			pData->metric = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
   		} else if(!strcmp(actpblk.descr[i].name, "description")) {
			pData->description = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
   		} else if(!strcmp(actpblk.descr[i].name, "subtree")) {
			pData->propSubtree = getPropertyDescriptor((uchar*)es_str2cstr(pvals[i].val.d.estr, NULL));
		} else if (!strcmp(actpblk.descr[i].name, "mode")) {
            pData->mode = strcmp ("single", es_str2cstr(pvals[i].val.d.estr, NULL));
   		} else if (!strcmp(actpblk.descr[i].name, "prefix")) {
            pData->prefix = es_str2cstr(pvals[i].val.d.estr, NULL);
            pData->prefixLen = strlen(pData->prefix);
        }
 }
    pData->propHost = getPropertyDescriptor(pData->host);
    pData->propService = getPropertyDescriptor(pData->service);
    pData->propMetric = getPropertyDescriptor(pData->metric);
    pData->propDescription = getPropertyDescriptor(pData->description);
    pData->propTime = getPropertyDescriptor(pData->time);
	
	CODE_STD_STRING_REQUESTnewActInst(1);
    CHKiRet(OMSRsetEntry(*ppOMSR, 0, NULL, OMSR_TPL_AS_MSG));


CODE_STD_FINALIZERnewActInst
	cnfparamvalsDestruct(pvals, &actpblk);
ENDnewActInst

BEGINparseSelectorAct
CODESTARTparseSelectorAct
CODE_STD_STRING_REQUESTparseSelectorAct(1)
CODE_STD_FINALIZERparseSelectorAct
ENDparseSelectorAct


BEGINmodExit
CODESTARTmodExit
ENDmodExit


BEGINqueryEtryPt
CODESTARTqueryEtryPt
CODEqueryEtryPt_STD_OMOD_QUERIES
CODEqueryEtryPt_STD_OMOD8_QUERIES
CODEqueryEtryPt_STD_CONF2_OMOD_QUERIES
ENDqueryEtryPt

BEGINmodInit()
CODESTARTmodInit
	*ipIFVersProvided = CURR_MOD_IF_VERSION; /* only supports rsyslog 6 configs */
CODEmodInit_QueryRegCFSLineHdlr
	CHKiRet(objUse(errmsg, CORE_COMPONENT));
	INITChkCoreFeature(bCoreSupportsBatching, CORE_FEATURE_BATCHING);
	if (!bCoreSupportsBatching) {
		errmsg.LogError(0, NO_ERRCODE, "omhiredis: rsyslog core does not support batching - abort");
		ABORT_FINALIZE(RS_RET_ERR);
	}
ENDmodInit
