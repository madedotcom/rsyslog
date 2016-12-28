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

typedef struct _instanceData {
  uchar *server;
  int port;

  uchar *host;
  uchar *time;
  uchar *service;
  uchar *metric;

  msgPropDescr_t *propHost;
  msgPropDescr_t *propTime;
  msgPropDescr_t *propService;
  msgPropDescr_t *propMetric; 
  msgPropDescr_t *propSubtree;

} instanceData;

typedef struct wrkrInstanceData {
	instanceData *pData; 
	riemann_client_t *client; 
  int count;
} wrkrInstanceData_t;

typedef struct eventlist {
  void **fields;
  struct eventlist *next;
} eventlist_t;

static pthread_mutex_t mutDoAct = PTHREAD_MUTEX_INITIALIZER;

static struct cnfparamdescr actpdescr[] = {
  { "server", eCmdHdlrGetWord, 0 },
  { "subtree", eCmdHdlrGetWord, 0 },
  { "serverport", eCmdHdlrInt, 0 },
  { "service", eCmdHdlrGetWord, 0 },
  { "metric", eCmdHdlrGetWord, 0 },
  { "host", eCmdHdlrGetWord, 0 },
  { "time", eCmdHdlrGetWord, 0 },
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

static eventlist_t*
eventlist_new(eventlist_t *tail) {
   eventlist_t * result;
   void ** fields;

   fields = calloc(16, sizeof(void *));
   result = calloc(1, sizeof(eventlist_t *));

   result->fields = fields;
   result->next = tail;

   dbgprintf("made a new list\n");

   return result;
}

static void eventlist_free(eventlist_t *list)
{
   eventlist_t *tmp;

   while (list != NULL)
   {
      free(list->fields);

      tmp = list;
      list = list->next;
      free(tmp);
   }
}

static void setField(void **event, smsg_t *msg,
                short unsigned isMandatory, riemann_event_field_t field, 
                uchar *cfgValue, msgPropDescr_t *resolvedProperty) {
    
        uchar *propValue; 
        short unsigned mustFree; 
        rs_size_t propLen;
        propValue = NULL;

        if(NULL != event[field])
          return;

        // if we have a reslved property for this field, then everything is simple.
        if (NULL != resolvedProperty)
        {
           propValue = MsgGetProp(msg, NULL, resolvedProperty, &propLen, &mustFree, NULL);
           if( NULL != propValue)
              event[field] = strdup((char *)propValue);
        } else if(isMandatory){
          // if this field is mandatory, then we'll assume that the configured value is a literal
          event[field] = strdup((char *)cfgValue);
        }

        if(mustFree)
            free(propValue);
}

static unsigned short setMetricFromString(const char* value, eventlist_t *event)
{
    double dValue;
    int64_t iValue;
    char* errPtr;

    iValue = strtol((const char *)value, &errPtr, 10);

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

    if('\0' == *errPtr) {
       event->fields[RIEMANN_EVENT_FIELD_METRIC_D] = calloc(1, sizeof(double));
       *((double*)event->fields[RIEMANN_EVENT_FIELD_METRIC_D]) = dValue;
       return 1;
    }
    return 0;
}

static unsigned short setMetricFromJsonValue(struct json_object *json, eventlist_t *event)
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
            event->fields[RIEMANN_EVENT_FIELD_METRIC_S64] = calloc(1, sizeof(int64_t));
            *((int64_t *)event->fields[RIEMANN_EVENT_FIELD_METRIC_S64]) = json_object_get_int64(json);
            return 1;
          case json_type_double:
            dbgprintf("I am handling a double!");
            event->fields[RIEMANN_EVENT_FIELD_METRIC_D] = calloc(1, sizeof(double));
            *((double *)event->fields[RIEMANN_EVENT_FIELD_METRIC_D]) = json_object_get_double(json);
            return 1;
          case json_type_string:
            dbgprintf("I am handling a string!");
            return setMetricFromString(json_object_get_string(json), event);
       }
       return 0;
}


static void 
setFieldsFromConfig(eventlist_t *event, smsg_t *msg, instanceData *cfg)
{
    setField(event->fields, msg, 1, RIEMANN_EVENT_FIELD_HOST, cfg->host, cfg->propHost);
    setField(event->fields, msg, 1, RIEMANN_EVENT_FIELD_SERVICE, cfg->service, cfg->propService);

    if( NULL != event->fields[RIEMANN_EVENT_FIELD_METRIC_S64] || NULL != event->fields[RIEMANN_EVENT_FIELD_METRIC_D]) 
       return;
    

    int localRet;
    uchar* value;
    rs_size_t valueLen;
    unsigned short mustFree;

    struct json_object *json;

    if (NULL == cfg->propMetric) {
       setMetricFromString((char *)cfg->metric, event);
    }

    else {
        // we resolved a property earlier, so now we need to get it.
        switch(cfg->propMetric->id) {
          case PROP_CEE:
          case PROP_CEE_ALL_JSON:
          case PROP_CEE_ALL_JSON_PLAIN:
            
            localRet = msgGetJSONPropJSON(msg, cfg->propMetric, &json);
            if (localRet == RS_RET_OK)
              setMetricFromJsonValue(json, event);
            break;
          default:
             value = MsgGetProp(msg, NULL, cfg->propMetric, &valueLen, &mustFree, NULL);
             setMetricFromString((char *)value, event);
             if(mustFree) 
               free(value);
        }
    }
}

static void setServiceName(void **fields, const char* instanceName, int instanceNameLen, const char* metricName) {
    
    if (instanceNameLen == 0) {
       fields[RIEMANN_EVENT_FIELD_SERVICE] = strdup(instanceName);
    }
    else
    {
        size_t metricNameLen = strlen(metricName);
        char* serviceName = malloc( sizeof(char) * (instanceNameLen + metricNameLen + 1) );
        strcpy(serviceName, instanceName);
        strcat(serviceName, "/");
        strcat(serviceName, metricName);
        dbgprintf("service name is %s", serviceName);
        fields[RIEMANN_EVENT_FIELD_SERVICE] = serviceName;
    } 
}


static eventlist_t *
makeEventsFromMessage(smsg_t *msg, instanceData *cfg) 
{
    eventlist_t * list = NULL; 
    eventlist_t *next = NULL;
    struct json_object *json = NULL;
    struct json_object_iterator it;
    struct json_object_iterator itEnd;
    struct json_object *val;
    enum json_type type;
    char* instanceName = NULL;
    int instanceNameLen = 0;
    int err;

    // This is the simple case. If we have no json subtree
    // then we're only sending a single event, based on the fields
    // that are defined in the config.
    if (NULL == cfg->propSubtree)
    {
        list = eventlist_new(NULL);
        setFieldsFromConfig(list, msg, cfg);
        return list;
    }


    err = msgGetJSONPropJSON(msg, cfg->propSubtree, &json);
    dbgprintf("GetPropJSON returned %d", err);

    if (NULL == json)
    {
        dbgprintf("Json value is null, falling back to defaults");
        list = eventlist_new(NULL);
        setFieldsFromConfig(list, msg, cfg);
        return list;
    }

    json_object_object_get_ex(json, "name", &val);
    if(NULL != val) {
      type = json_object_get_type(val);
      dbgprintf("I've got a name field of type %d\n", type);
      if (type == json_type_string) {
         instanceName = json_object_get_string(val);
         instanceNameLen = strlen(instanceName);
      }
    }

    it = json_object_iter_begin(json);
    itEnd = json_object_iter_end(json);

    next = eventlist_new(NULL);
    while( !json_object_iter_equal(&it, &itEnd) )
    {
       val = json_object_iter_peek_value(&it);
       dbgprintf("handling %s", json_object_iter_peek_name(&it));
       type = json_object_get_type(val);
       if (setMetricFromJsonValue(val, next)) {
          setServiceName(next->fields, instanceName, instanceNameLen, json_object_iter_peek_name(&it));
          setFieldsFromConfig(next, msg, cfg);
          next->next = list;
          list = next;
          next = eventlist_new(NULL);
       }
       json_object_iter_next(&it);
    }
    if (NULL == next->next)
       eventlist_free(next);
    return list;
}

static void copyField(eventlist_t *src, riemann_event_t *event, riemann_event_field_t field)
{
    if( NULL != src->fields[field] )
      riemann_event_set(event, field, src->fields[field]);
}

static void copyIntField(eventlist_t *src, riemann_event_t *event, riemann_event_field_t field)
{
    if( NULL != src->fields[field] )
      riemann_event_set(event, field, *((int64_t*)src->fields[field]));
}

static riemann_message_t*
serializeEvents(eventlist_t *root)
{
    riemann_message_t *msg;
    int num_events = 0;
    int i = 0;
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
       copyField(current, events[i], RIEMANN_EVENT_FIELD_HOST);
       copyField(current, events[i], RIEMANN_EVENT_FIELD_SERVICE);
       copyIntField(current, events[i], RIEMANN_EVENT_FIELD_METRIC_D);
       copyIntField(current, events[i], RIEMANN_EVENT_FIELD_METRIC_S64);
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

static void
setPropertyDescriptor(msgPropDescr_t **prop, uchar *name)
{
    
    propid_t prop_id;
    propNameToID(name, &prop_id);
    
    if (prop_id == PROP_INVALID) 
    {
       *prop = NULL;
       return;
    }

    *prop = (msgPropDescr_t*) calloc(1, sizeof *prop);
    msgPropDescrFill(*prop, name, strlen((const char *)name));
}

static void
setInstParamDefaults(instanceData *pData)
{
	pData->server = "localhost";
	pData->port = 5555;
    pData->host = "hostname";
    pData->time = "timestamp";
    pData->service = "programname";
    pData->metric = "1";
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
		} else if(!strcmp(actpblk.descr[i].name, "metric")) {
			pData->metric = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
   		} else if(!strcmp(actpblk.descr[i].name, "subtree")) {
			setPropertyDescriptor( &pData->propSubtree, (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL));
		} 
 }
    setPropertyDescriptor(&pData->propHost, pData->host);
    setPropertyDescriptor(&pData->propService, pData->service);
    setPropertyDescriptor(&pData->propMetric, pData->metric);
    setPropertyDescriptor(&pData->propTime, pData->time);
	
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
