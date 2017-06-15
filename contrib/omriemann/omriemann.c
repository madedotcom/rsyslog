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

typedef struct _eventState {
  riemann_event_t *event;
  unsigned short hasHost;
  unsigned short hasTime;
  unsigned short hasTtl;
  unsigned short hasService;
  unsigned short hasState;
  unsigned short hasMetric;
  unsigned short hasDescription;
} eventState;

static eventState*
eventStateNew(riemann_event_t *event){
        eventState *result = (eventState*) malloc(sizeof(eventState));
        result->event = event;
        result->hasHost = 0;
        result->hasTime = 0;
        result->hasTtl = 0;
        result->hasService = 0;
        result->hasMetric = 0;
        result->hasState = 0;
        result->hasDescription = 0;
        return result;
}

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


static const uchar *
readConfigValue (smsg_t *msg, uchar *cfgValue, msgPropDescr_t *resolvedProperty, unsigned short *mustFree) {
        uchar *propValue;
        rs_size_t propLen;
        propValue = NULL;

        // if we have a resolved property for this field, use the property value
        if (NULL != resolvedProperty)
        {
           propValue = MsgGetProp(msg, NULL, resolvedProperty, &propLen, mustFree, NULL);
           if( NULL != propValue)
              return propValue;
        }
        if (*mustFree) {
           free(propValue);
           *mustFree = 0;
        }
        return cfgValue;
}

static void
setFieldFromConfig(eventState * state, smsg_t *msg,
                riemann_event_field_t field, uchar *cfgValue,
                msgPropDescr_t *resolvedProperty) {

        unsigned short mustFree = 0;
        const uchar* value = readConfigValue(msg, cfgValue, resolvedProperty, &mustFree);
        riemann_event_set(state->event, field, value, RIEMANN_EVENT_FIELD_NONE);
        if(mustFree)
          free((void*)value);
}

/* Parse a string and set either the metric_s64 or metric_d fields in the event*/
static void
setMetricFromString(const char* value, eventState *state)
{
    double dValue;
    int64_t iValue;
    char* errPtr;

    iValue = strtol((const char *)value, &errPtr, 10);

    // if we parsed to the end of the string, then we have a simple integer.
    if ('\0' == *errPtr) {
      riemann_event_set(state->event, RIEMANN_EVENT_FIELD_METRIC_S64, iValue, RIEMANN_EVENT_FIELD_NONE);
      state->hasMetric = 1;
    }

    // we were given no characters at all, so we won't set a metric field.
    if(errPtr == value)
      return;

    // if we failed on a decimal point, let's try parsing this as a decimal.
    if (*errPtr == '.')
    {
      errPtr = NULL;
      dValue = strtod(value, &errPtr);
    }

    // if we reached the end of the string, we have a decimal.
    if('\0' == *errPtr) {
       riemann_event_set(state->event, RIEMANN_EVENT_FIELD_METRIC_D, dValue, RIEMANN_EVENT_FIELD_NONE);
       state->hasMetric = 1;
    }

    // otherwise we've got a non-numeric string.
}

/* Parses a json rValue and sets the metric_s64 or metric_d fields */
static void
setMetricFromJsonValue(struct json_object *json, eventState *state)
{
        json_type type;

        if (NULL == json)
           return;

        type = json_object_get_type(json);

        switch(type) {
          case json_type_null:
          case json_type_array:
          case json_type_object:
            // honestly can't give you a metric in this situation.
            // sorry.
            return;
          case json_type_boolean:
          case json_type_int:
            // we'll treat bool as a special case of integer.
            riemann_event_set(state->event, RIEMANN_EVENT_FIELD_METRIC_S64, json_object_get_int64(json));
            state->hasMetric = 1;
          case json_type_double:
            // double is simple.
            riemann_event_set(state->event, RIEMANN_EVENT_FIELD_METRIC_D, json_object_get_double(json));
            state->hasMetric = 1;
          case json_type_string:
            // if we were given a string, we'll try and parse it.
            setMetricFromString(json_object_get_string(json), state);
       }
}

/* Take the config setting for `metric` and parse it to set a metric value*/
static void
setMetricFromConfig(eventState* state, smsg_t *msg, instanceData *cfg)
{
    // If the metric has already been set, then we won't override it.
    if (state->hasMetric) {
       return;
    }

    int localRet;
    uchar* value;
    rs_size_t valueLen;
    unsigned short mustFree;

    struct json_object *json;

    // If there is no resolved property for metric then we'll treat metric as a literal string
    // This is the default case since we set metric to "1" if it isn't provided by a user.
    if (NULL == cfg->propMetric) {
       setMetricFromString((char *)cfg->metric, state);
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
              setMetricFromJsonValue(json, state);
            break;
          // otherwise we'll parse it from the string.
          default:
             value = MsgGetProp(msg, NULL, cfg->propMetric, &valueLen, &mustFree, NULL);
             setMetricFromString((char *)value, state);
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
setFieldsFromConfig(eventState *state, smsg_t *msg, instanceData *cfg)
{
    if(!state->hasHost) {
        setFieldFromConfig(state, msg, RIEMANN_EVENT_FIELD_HOST, cfg->host, cfg->propHost);
    }
    if(!state->hasService) {
            setFieldFromConfig(state, msg, RIEMANN_EVENT_FIELD_SERVICE, cfg->service, cfg->propService);
    }
    if(!state->hasDescription) {
            setFieldFromConfig(state, msg, RIEMANN_EVENT_FIELD_DESCRIPTION, cfg->description, cfg->propDescription);
    }
    if(!state->hasMetric) {
        setMetricFromConfig(state, msg, cfg);
    }
}


/* If we receive an impstats message, we use this function to set the service according to the `name` field and they key of the metric.
 * eg: given the json data {name="action 0", processed=10, failed=2} we will send two events with 
 * the service names "action 0/processed" and "action 0/failed"
 *
 * This function also handles prefixing service names, eg. action(type="omriemann" prefix="foo") will yield metrics with service names
 * `foo/$programname`
 */
static void
setServiceName(eventState* state, const char* prefix, size_t prefixLen, const char* instanceName, size_t instanceNameLen, const char* metricName) {

    int offset = 0;
    size_t metricNameLen = 0;

    if(state->hasService) {
        return;
    }

    if (instanceNameLen == 0 && prefixLen == 0) {
       riemann_event_set(state->event, RIEMANN_EVENT_FIELD_SERVICE, metricName, RIEMANN_EVENT_FIELD_NONE);
       state->hasService = 1;
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
       riemann_event_set(state->event, RIEMANN_EVENT_FIELD_SERVICE, serviceName, RIEMANN_EVENT_FIELD_NONE);
       state->hasService = 1;
    }
}

static int buildSingleEvent(instanceData *cfg, eventState *state, json_object *root)
{
    struct json_object_iterator it;
    struct json_object_iterator itEnd;
    struct json_object *val;
    struct json_object *tag;
    enum json_type type;
    const char* name = NULL;
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
            setServiceName(state, cfg->prefix, cfg->prefixLen, NULL, 0, json_object_get_string(val));
            hasValues = 1;
       }
       else if(!strcmp(name, "metric")) {
            setMetricFromJsonValue(val, state);
            hasValues = 1;
       }
       else if (!strcmp(name, "state")) {
            riemann_event_set(state->event, RIEMANN_EVENT_FIELD_STATE, json_object_get_string(val), RIEMANN_EVENT_FIELD_NONE);
            state->hasState = 1;
            hasValues = 1;
       }
       else if (!strcmp(name, "description")) {
            riemann_event_set(state->event, RIEMANN_EVENT_FIELD_DESCRIPTION, json_object_get_string(val), RIEMANN_EVENT_FIELD_NONE);
            state->hasDescription = 1;
            hasValues = 1;
       }
       else if (!strcmp(name, "host")) {
            riemann_event_set(state->event, RIEMANN_EVENT_FIELD_HOST, json_object_get_string(val), RIEMANN_EVENT_FIELD_NONE);
            state->hasHost = 1;
            hasValues = 1;
       }
       else if (!strcmp(name, "ttl") && (type == json_type_double)) {
            riemann_event_set(state->event, RIEMANN_EVENT_FIELD_TTL, json_object_get_double(val), RIEMANN_EVENT_FIELD_NONE);
            state->hasTtl = 1;
            hasValues = 1;
       }
       else if (!strcmp(name, "ttl") && (type == json_type_int)) {
            riemann_event_set(state->event, RIEMANN_EVENT_FIELD_TTL, json_object_get_int(val), RIEMANN_EVENT_FIELD_NONE);
            state->hasTtl = 1;
            hasValues = 1;
       }
       else if (!strcmp(name, "tags") && type == json_type_array) {

            len = json_object_array_length(val);
            for(i = 0; i < len; i++)
            {
                tag = json_object_array_get_idx(val, i);
                type = json_object_get_type(tag);
                if (type == json_type_string) {
                   riemann_event_tag_add(state->event, json_object_get_string(tag));
                }
            }
       }
       else if (!strcmp(name, "attributes") && type == json_type_object) {
           struct json_object_iterator attribsIt = json_object_iter_begin(val);;
           struct json_object_iterator attribsItEnd = json_object_iter_end(val);

           i = 0;

           while (!json_object_iter_equal(&attribsIt, &attribsItEnd))
           {
               val = json_object_iter_peek_value(&attribsIt);
               name = json_object_iter_peek_name(&attribsIt);

               riemann_event_string_attribute_add(state->event, name, json_object_get_string(val));
               hasValues = 1;
               json_object_iter_next(&attribsIt);
           }
       }
       json_object_iter_next(&it);
    }

    if(NULL != val) {
        free(val);
    }

    return hasValues;
}


/* This is the function responsible for mapping a message to an eventlist_t. */
static unsigned short
makeEventsFromMessage(smsg_t *msg, riemann_message_t *riemann_msg, instanceData *cfg)
{
    eventState *state;
    struct json_object *json = NULL;
    struct json_object_iterator it;
    struct json_object_iterator itEnd;
    struct json_object_iterator valuesIt;
    struct json_object_iterator valuesItEnd;
    struct json_object *val;
    enum json_type type;
    const char* name = NULL;
    const char* instanceName = NULL;
    int instanceNameLen = 0;
    int err;
    unsigned short hasEvents = 0;
    unsigned short hasValues = 0;
    riemann_event_t * event;

    event = riemann_event_new();
    state = eventStateNew(event);
    hasValues = 0;

    // This is the simple case. If we have no json subtree
    // then we're only sending a single event, based on the fields
    // that are defined in the config.
    if (NULL == cfg->propSubtree)
    {
        unsigned short mustFree = 0;
        name = (char *)readConfigValue(msg, cfg->service, cfg->propService, &mustFree);
        setServiceName(state, cfg->prefix, cfg->prefixLen, instanceName, instanceNameLen, name);
        setFieldsFromConfig(state, msg, cfg);
        riemann_message_set_events(riemann_msg, event, NULL);
        free(state);
        if(mustFree) { free(name); }
        return 1;
    }

    // If we have a subtree configured, but we're unable to parse it
    // for whatever reason, we'll fall back to just sending default fields.
    err = msgGetJSONPropJSON(msg, cfg->propSubtree, &json);
    if (NULL == json || err != RS_RET_OK)
    {
        unsigned short mustFree = 0;
        name = (char *)readConfigValue(msg, cfg->service, cfg->propService, &mustFree);
        setServiceName(state, cfg->prefix, cfg->prefixLen, instanceName, instanceNameLen, name);
        setFieldsFromConfig(state, msg, cfg);
        riemann_message_set_events(riemann_msg, event, NULL);
        if(mustFree) { free(name); }
        return 1;
    }


    // If we're in single-event mode then try to pase the subtree as
    // a single event.
    if(cfg->mode == 0)
    {
        if(buildSingleEvent(cfg, state, json))
        {
          setFieldsFromConfig(state, msg, cfg);
          riemann_message_set_events(riemann_msg, event, NULL);
          hasEvents = 1;
        }
        else {
          riemann_event_free(event);
          hasEvents = 0;
        }
        free(state);
        free(json);
        return hasEvents;
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
            setMetricFromJsonValue(val, state);
            if (state->hasMetric) {
                  setServiceName(state, cfg->prefix, cfg->prefixLen, instanceName, instanceNameLen, name);
                  setFieldsFromConfig(state, msg, cfg);
                  riemann_message_append_events(riemann_msg, event);
                  hasValues = hasEvents = 1;
                  free(state);
                  event = riemann_event_new();
                  state = eventStateNew(event);
            }
            json_object_iter_next(&valuesIt);
          }
       }
       else {
         setMetricFromJsonValue(val, state);
         if (state->hasMetric) {
                  setServiceName(state, cfg->prefix, cfg->prefixLen, instanceName, instanceNameLen, name);
                  setFieldsFromConfig(state, msg, cfg);
                  riemann_message_append_events(riemann_msg, event);
                  hasValues = hasEvents = 1;
                  free(state);
                  event = riemann_event_new();
                  state = eventStateNew(event);
         }
       }
       json_object_iter_next(&it);
    }
    if( NULL != val)
       free(val);
    free(json);
    if (0 == hasValues)
       riemann_event_free(event);
    return hasEvents;
}

rsRetVal enqueueRiemannMetric(smsg_t *pMsg, wrkrInstanceData_t *pWrkrData) {
    DEFiRet;
    instanceData *cfg;
    riemann_message_t *riemannMessage = riemann_message_new();
    unsigned short hasEvents;
    cfg = pWrkrData->pData;

    hasEvents = makeEventsFromMessage(pMsg, riemannMessage, cfg);
    if( !hasEvents)
    {
       ABORT_FINALIZE(RS_RET_OK);
    }

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
