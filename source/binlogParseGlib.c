



#include <endian.h>
#include <glib.h>
#include <unistd.h>
#include <inttypes.h>
#include <dirent.h>
#include <regex.h>


#define MAX_HEADER_LENGTH 200
#define EVENT_HEADER_LENGTH 19
#define CHECKSUM_LENGTH 4

#define G_LOG_DOMAIN    ((gchar*) 0)
//static FormatDescriptionEvent* formatDescriptionEventForGlobalUse=NULL;


static gchar MAGIC_HEADER_CONTENT[4]={'\xfe','b','i','n'};


static gchar* optDatabaseNames=NULL;
static gchar* optTableNames=NULL;
static guint64 optStartPos=0;
static guint64 optStopPos=0;
static gchar* optStartDatetimeStr=NULL;
static gchar* optStopDatetimeStr=NULL;
static gchar* optSqlTypes=NULL;
static guint64 optMaxSplitSize=0;
static gchar* optBinlogFiles=NULL;
static gchar* optOutBinlogFileNameBase=NULL;
static gchar* optLogLevel=NULL;
static gchar* optIncludeGtids=NULL;
static gchar* optExcludeGtids=NULL;


static time_t globalStartTimestamp=0;
static time_t globalStopTimestamp=0;
static GArray* globalIncludeGtidsArray=NULL;
static GArray* globalExcludeGtidsArray=NULL;

static GHashTable* tableNamesHash=NULL;
static GHashTable* databaseNamesHash=NULL;





enum Status_Stop_Discard
{
  GOON=0,
  STOP=1,
  DISCARD=2
};


enum Binlog_event_type
{
  UNKNOWN_EVENT= 0,
  START_EVENT_V3= 1,
  QUERY_EVENT= 2,
  STOP_EVENT= 3,
  ROTATE_EVENT= 4,
  INTVAR_EVENT= 5,
  LOAD_EVENT= 6,
  SLAVE_EVENT= 7,
  CREATE_FILE_EVENT= 8,
  APPEND_BLOCK_EVENT= 9,
  EXEC_LOAD_EVENT= 10,
  DELETE_FILE_EVENT= 11,
  NEW_LOAD_EVENT= 12,
  RAND_EVENT= 13,
  USER_VAR_EVENT= 14,
  FORMAT_DESCRIPTION_EVENT= 15,
  XID_EVENT= 16,
  BEGIN_LOAD_QUERY_EVENT= 17,
  EXECUTE_LOAD_QUERY_EVENT= 18,

  TABLE_MAP_EVENT = 19,
  PRE_GA_WRITE_ROWS_EVENT = 20,
  PRE_GA_UPDATE_ROWS_EVENT = 21,
  PRE_GA_DELETE_ROWS_EVENT = 22,

  WRITE_ROWS_EVENT_V1 = 23,
  UPDATE_ROWS_EVENT_V1 = 24,
  DELETE_ROWS_EVENT_V1 = 25,

  INCIDENT_EVENT= 26,

  HEARTBEAT_LOG_EVENT= 27,

  IGNORABLE_LOG_EVENT= 28,
  ROWS_QUERY_LOG_EVENT= 29,

  WRITE_ROWS_EVENT = 30,
  UPDATE_ROWS_EVENT = 31,
  DELETE_ROWS_EVENT = 32,

  GTID_LOG_EVENT= 33,
  ANONYMOUS_GTID_LOG_EVENT= 34,

  PREVIOUS_GTIDS_LOG_EVENT= 35,
  TRANSACTION_CONTEXT_EVENT= 36,

  VIEW_CHANGE_EVENT= 37,

  XA_PREPARE_LOG_EVENT= 38,
  ENUM_END_EVENT
};

static gchar Binlog_event_type_name[][30]={
  "UNKNOWN_EVENT",
  "START_EVENT_V3",
  "QUERY_EVENT",
  "STOP_EVENT",
  "ROTATE_EVENT",
  "INTVAR_EVENT",
  "LOAD_EVENT",
  "SLAVE_EVENT",
  "CREATE_FILE_EVENT",
  "APPEND_BLOCK_EVENT",
  "EXEC_LOAD_EVENT",
  "DELETE_FILE_EVENT",
  "NEW_LOAD_EVENT",
  "RAND_EVENT",
  "USER_VAR_EVENT",
  "FORMAT_DESCRIPTION_EVENT",
  "XID_EVENT",
  "BEGIN_LOAD_QUERY_EVENT",
  "EXECUTE_LOAD_QUERY_EVENT",

  "TABLE_MAP_EVENT",
  "PRE_GA_WRITE_ROWS_EVENT",
  "PRE_GA_UPDATE_ROWS_EVENT",
  "PRE_GA_DELETE_ROWS_EVENT",

  "WRITE_ROWS_EVENT_V1",
  "UPDATE_ROWS_EVENT_V1",
  "DELETE_ROWS_EVENT_V1",

  "INCIDENT_EVENT",

  "HEARTBEAT_LOG_EVENT",

  "IGNORABLE_LOG_EVENT",
  "ROWS_QUERY_LOG_EVENT",

  "WRITE_ROWS_EVENT",
  "UPDATE_ROWS_EVENT",
  "DELETE_ROWS_EVENT",
  "GTID_LOG_EVENT",
  "ANONYMOUS_GTID_LOG_EVENT",

  "PREVIOUS_GTIDS_LOG_EVENT",
  "TRANSACTION_CONTEXT_EVENT",

  "VIEW_CHANGE_EVENT",

  "XA_PREPARE_LOG_EVENT",
  "ENUM_END_EVENT"

};

typedef enum enum_dml_types {
  INSERT =0,
  UPDATE =1,
  DELETE =2,
  UNKNOWN_TYPE
} enum_dml_types;

typedef enum enum_field_types {
  MYSQL_TYPE_DECIMAL, MYSQL_TYPE_TINY,
  MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG,
  MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE,
  MYSQL_TYPE_NULL, MYSQL_TYPE_TIMESTAMP,
  MYSQL_TYPE_LONGLONG,MYSQL_TYPE_INT24,
  MYSQL_TYPE_DATE, MYSQL_TYPE_TIME,
  MYSQL_TYPE_DATETIME, MYSQL_TYPE_YEAR,
  MYSQL_TYPE_NEWDATE, MYSQL_TYPE_VARCHAR,
  MYSQL_TYPE_BIT,
  MYSQL_TYPE_TIMESTAMP2,
  MYSQL_TYPE_DATETIME2,
  MYSQL_TYPE_TIME2,
  MYSQL_TYPE_JSON=245,
  MYSQL_TYPE_NEWDECIMAL=246,
  MYSQL_TYPE_ENUM=247,
  MYSQL_TYPE_SET=248,
  MYSQL_TYPE_TINY_BLOB=249,
  MYSQL_TYPE_MEDIUM_BLOB=250,
  MYSQL_TYPE_LONG_BLOB=251,
  MYSQL_TYPE_BLOB=252,
  MYSQL_TYPE_VAR_STRING=253,
  MYSQL_TYPE_STRING=254,
  MYSQL_TYPE_GEOMETRY=255
} enum_field_types;


typedef enum enum_flag
  {
  /* Last event of a statement */
  STMT_END_F = (1U << 0),

  /* Value of the OPTION_NO_FOREIGN_KEY_CHECKS flag in thd->options */
  NO_FOREIGN_KEY_CHECKS_F = (1U << 1),

  /* Value of the OPTION_RELAXED_UNIQUE_CHECKS flag in thd->options */
  RELAXED_UNIQUE_CHECKS_F = (1U << 2),

  /**
  Indicates that rows in this event are complete, that is contain
  values for all columns of the table.
  */
  COMPLETE_ROWS_F = (1U << 3)
  } enum_flag;

typedef struct _GtidSetInfo{
  gchar* uuid;
  guint64 startSeqNo;
  guint64 stopSeqNo;
} GtidSetInfo;

typedef struct _EventHeader{
	guint32 binlogTimestamp;
	guint8  eventType;
	guint32 serverId;
	guint32 eventLength;
	guint64 nextEventPos;
	guint16  flag;
	gchar *rawEventHeader;
} EventHeader;

typedef struct _FormatDescriptionEvent{
	EventHeader *eventHeader;
	gchar *rawFormatDescriptionEventDataDetail;
} FormatDescriptionEvent;

typedef struct _TableMapEvent{
	EventHeader           *eventHeader;
	gchar                 *rawTableMapEventDataDetail;
  guint64 databaseNameLength;
  gchar* databaseName;
  guint64 tableNameLength;
	gchar* tableName;
	guint64 tableId;
	guint64 columnNumber;
	GByteArray            *columnTypeArray;
  guint16               *columnMetadataArray;
  guint64                  metadataBlockSize;
} TableMapEvent;

typedef struct _RowEvent{
	EventHeader           *eventHeader;
	gchar                 *rawRowEventDataDetail;
} RowEvent;

typedef struct _QueryEvent{
  EventHeader           *eventHeader;
  gchar                 *rawQueryEventDataDetail;
  guint64                 databaseNameLength;
  gchar                 *databaseName;
  gchar                 sqlTextLength;
  gchar                 *sqlText;
} QueryEvent;


typedef struct _GtidEvent{
  EventHeader           *eventHeader;
  gchar                 *rawGtidEventDataDetail;
  gchar                 *uuid;
  guint64               seqNo;
} GtidEvent;



typedef struct _XidEvent{
  EventHeader           *eventHeader;
  gchar                 *rawXidEventDataDetail;
  guint64               xid;
} XidEvent;

typedef struct _EventWrapper{
  guint8    eventType;
  gpointer  eventPointer;
} EventWrapper;


typedef struct _LeastExecutionUnitEvents{
	TableMapEvent *tableMapEvent;
  GList         *rowEventList;
  guint8        originalRowEventType;
} LeastExecutionUnitEvents;

static FormatDescriptionEvent* formatDescriptionEventForGlobalUse=NULL;
static XidEvent* xidEventForGlobalUse=NULL;

/*
typedef struct _Transaction{
	GList leastExecutionUnitEventsList;
} Transaction;

*/


gboolean isConsideredEventType(guint8 eventType){
  gboolean isConsidered=FALSE;
  switch(eventType){
    case FORMAT_DESCRIPTION_EVENT:{
      isConsidered=TRUE;
      break;
    }
    case TABLE_MAP_EVENT:{
      isConsidered=TRUE;
      break;
    }
    case WRITE_ROWS_EVENT:
    case UPDATE_ROWS_EVENT:
    case DELETE_ROWS_EVENT:{
      isConsidered=TRUE;
      break;
    }
    case QUERY_EVENT:{
      isConsidered=TRUE;
      break;
    }
    case XID_EVENT:{
      isConsidered=TRUE;
      break;
    }
    case GTID_LOG_EVENT:{
      isConsidered=TRUE;
      break;
    }
  }

  return isConsidered;
}


gboolean checkPotentialConflictOutputFile(gchar* baseName){
    int reti;
    regex_t regex;
    reti = regcomp(&regex, baseName, 0);
    if(reti){
      g_error("failed to compile regex %s", baseName);
    }
    DIR           *d;
    struct dirent *dir;
    d = opendir(".");
    if (d)
    {
      while ((dir = readdir(d)) != NULL)
      {
        if (!regexec(&regex, dir->d_name, 0, NULL, 0)){
          g_error("the output %s.* may overwrite the existing file %s, please choose a newFileName specified by --outBinlogFileNameBase or remove the existing file",baseName,dir->d_name);
          return FALSE;
        }
      }

      closedir(d);
    }

    return TRUE;
}

gchar* constructFileNameWithPostfixIndex(gchar* baseName, guint64 postfixIndex){
  gchar* completeFileName;
  //completeFileName=g_new0(gchar,strlen(baseName)+postfixDisplayLength+1+1);
  if(0 == postfixIndex){
    completeFileName=g_strdup_printf("%s",baseName);
  }
  else{
    completeFileName=g_strdup_printf("%s.%06lu",baseName,postfixIndex);
  }
  return completeFileName;
}

gboolean rotateFile(gchar* baseName, guint64 postfixIndex){
    gchar* completeFileName=constructFileNameWithPostfixIndex(baseName,postfixIndex);
    if( -1 == access( completeFileName, F_OK )  ) {
      return FALSE;
    }
    rotateFile(baseName,postfixIndex+1);
    gchar* newFileName=constructFileNameWithPostfixIndex(baseName,postfixIndex+1);
    gchar* oldFileName=constructFileNameWithPostfixIndex(baseName,postfixIndex);
    if(0 != rename(oldFileName,newFileName) ){
      g_error("unable to rename %s to %s",oldFileName, newFileName);
    }
    return TRUE;
}

gboolean rotateOutputBinlogFileNames(gchar* baseName, guint64 postfixIndex){
    gchar* flashbackBaseName;
    flashbackBaseName=g_strdup_printf("%s.%s",baseName,"flashback");
    rotateFile(flashbackBaseName,postfixIndex);
    return TRUE;
}

gboolean isGtidEventInGtidSet(GtidEvent* gtidEvent, GtidSetInfo* gtidSetInfo){
  if((0==memcmp(gtidEvent->uuid,gtidSetInfo->uuid,16)) ){
    if((gtidEvent->seqNo >= gtidSetInfo->startSeqNo) && ( gtidEvent->seqNo <=gtidSetInfo->stopSeqNo ) ){
      return TRUE;
    }
  }

  return FALSE;

}

int isGtidEventInGtidSetInfoArray(GtidEvent* gtidEvent, GArray* gtidSetInfoArray){

  guint64 i=0;
  GtidSetInfo* gtidSetInfo;
  for(;i<gtidSetInfoArray->len;i++){
    gtidSetInfo = &g_array_index(gtidSetInfoArray,GtidSetInfo,i);
    if(isGtidEventInGtidSet(gtidEvent,gtidSetInfo)){
      return TRUE;
    }
  }
  return FALSE;
}

gboolean isTransactionShouldDiscardForGtid(GtidEvent* gtidEvent){


  if((NULL ==globalIncludeGtidsArray) || (isGtidEventInGtidSetInfoArray(gtidEvent,globalIncludeGtidsArray)) ){
    if((NULL == globalExcludeGtidsArray) || (!isGtidEventInGtidSetInfoArray(gtidEvent,globalExcludeGtidsArray)) ){
      return FALSE;
    }
  }
   return TRUE;

}


gchar* packUuidInto16Bytes(gchar* originalUuid){
  gchar* binaryUuid=g_new0(gchar,16);
  guint64 originalUuidLen;
  originalUuidLen=strlen(originalUuid);
  guint64 effectiveCharIndex=0;
  int i=0;
  guint8 tempValue=0;
  for(i=0;i<originalUuidLen;i++){
    if('-' == originalUuid[i]){
      continue;
    }

    if( (originalUuid[i] >='0') &&  (originalUuid[i] <= '9' )){
      tempValue=originalUuid[i]-'0';
    }
    if( (g_ascii_tolower(originalUuid[i]) >='a') &&  (g_ascii_tolower(originalUuid[i]) <= 'f' ) ){
      tempValue=g_ascii_tolower(originalUuid[i]) - 'a' + 10;
    }

    if(0==(effectiveCharIndex%2)){
      tempValue = tempValue<<4;
    }
    binaryUuid[effectiveCharIndex/2]+=tempValue;
    effectiveCharIndex++;
  }
  return binaryUuid;
}


gboolean isDatabaseShouldApply(gchar* databaseName){
	if ( (NULL == databaseNamesHash) || g_hash_table_size(databaseNamesHash) <= 0){
		return TRUE;
	}
	if ( g_hash_table_lookup(databaseNamesHash, databaseName) ){
		return TRUE;
	}
	return FALSE; 
	
}

gboolean isTableShouldApply(gchar* tableName){
	if ( (NULL == tableNamesHash) ||  g_hash_table_size(tableNamesHash) <= 0){
		return TRUE;
	}
	if ( g_hash_table_lookup(tableNamesHash, tableName) ){
		return TRUE;
	}
	return FALSE; 
	
}

/*
gboolean isDatabaseShouldApply(gchar* databaseName){
  if (NULL == optDatabaseNames){
    return TRUE;
  }
  gchar **databaseNameArray;
  databaseNameArray=g_strsplit(optDatabaseNames,",",0);
  int i=0;
  while( databaseNameArray[i] ){
    if(0 == g_ascii_strcasecmp(databaseName,databaseNameArray[i])){
      return TRUE;
    }
    i++;
  }
  return FALSE;
}

gboolean isTableShouldApply(gchar* tableName){
  if (NULL == optTableNames){
    return TRUE;
  }
  gchar **tableNameArray;
  tableNameArray=g_strsplit(optTableNames,",",0);
  int i=0;
  while( tableNameArray[i] ){
    if(0 == g_ascii_strcasecmp(tableName,tableNameArray[i])){
      return TRUE;
    }
    i++;
  }
  return FALSE;
}
*/

gboolean isEventTypeShouldApply(guint8 eventType){
  if (NULL == optSqlTypes){
    return TRUE;
  }
  guint8 dmlTypeArray[4]={0};
  gchar **sqlTypeArray;
  sqlTypeArray=g_strsplit(optSqlTypes,",",0);
  int i=0;
  while( sqlTypeArray[i] ){
    if(0 == g_ascii_strcasecmp("INSERT",sqlTypeArray[i])){
      dmlTypeArray[INSERT]=1;
    }else if(0 == g_ascii_strcasecmp("UPDATE",sqlTypeArray[i])){
      dmlTypeArray[UPDATE]=1;
    }else if(0 == g_ascii_strcasecmp("DELETE",sqlTypeArray[i])){
      dmlTypeArray[DELETE]=1;
    }
    i++;
  }
  guint8 dmlType;
  switch (eventType) {
    case UPDATE_ROWS_EVENT:
      dmlType=UPDATE;
      break;
    case DELETE_ROWS_EVENT:
      dmlType=DELETE;
      break;
    case WRITE_ROWS_EVENT:
      dmlType=INSERT;
      break;
    default:
      dmlType=UNKNOWN_TYPE;
  }

  if( 0 ==dmlTypeArray[dmlType] ){
    return FALSE;
  }
  else{
    return TRUE;
  }

}

/*
gboolean isInDateTimeRange(guint32 timestamp){
  struct tm tm;
  time_t startTimeStamp;
  time_t stopTimeStamp;

  if(NULL != optStartDatetimeStr){
    if ( strptime(optStartDatetimeStr, "%Y-%m-%d %H:%M:%S", &tm) != NULL ){
      startTimeStamp = mktime(&tm);
    }else {
      g_error("failed to parsing startDatetimeStr");
    }
  }else {
    startTimeStamp=0;
  }

  if(NULL != optStopDatetimeStr){
    if ( strptime(optStopDatetimeStr, "%Y-%m-%d %H:%M:%S", &tm) != NULL ){
      stopTimeStamp = mktime(&tm);
    }else {
      g_error("failed to parsing stopDatetimeStr");
    }
  }else {
    stopTimeStamp=G_MAXUINT32;
  }

  if(timestamp >= startTimeStamp && timestamp <= stopTimeStamp){
    return TRUE;
  }

  return FALSE;

}
*/

gboolean isShouldStopOrDiscardForDateTimeRange(guint32 timestamp){

  if(timestamp <globalStartTimestamp){
    return DISCARD;
  }

  if(timestamp >globalStopTimestamp){
    return STOP;
  }

  return GOON;

}


gboolean getNextPosOrStop(guint64 *nextEventPos, guint64 fileIndex, gboolean isLastFile){

  gboolean isShouldStop=FALSE;

  if( (0 == fileIndex) && ((*nextEventPos) < optStartPos) ){
    *nextEventPos = optStartPos;
  }
  if( (TRUE == isLastFile) && (optStopPos >0) && ((*nextEventPos) >= optStopPos)  ){
    isShouldStop = TRUE;
  }

  return isShouldStop;
}

guint64 getRawEventDataLengthWithChecksum(EventHeader *eventHeader){
  guint64 rawEventDataLength;
  rawEventDataLength=eventHeader->eventLength - EVENT_HEADER_LENGTH; // - CHECKSUM_LENGTH
  return rawEventDataLength;
}


int setFormatDescriptionEventForGlobalUse(FormatDescriptionEvent* formatDescriptionEvent){
  formatDescriptionEventForGlobalUse=formatDescriptionEvent;
  return 0;
}

FormatDescriptionEvent* getFormatDescriptionEventForGlobalUse(){
  return formatDescriptionEventForGlobalUse;
}

int setXidEventForGlobalUse(XidEvent *xidEvent ){
  xidEventForGlobalUse=xidEvent;
  return 0;
}

int getXidEventForGlobalUse(){
  return xidEventForGlobalUse;
}

GIOChannel* getIoChannelForWrite(gchar* fileName){
  GIOChannel * toWriteChannel;
  toWriteChannel=g_io_channel_new_file(fileName,"w",NULL);
  if (NULL == toWriteChannel){
    g_warning("failed to create file %s", fileName);
    return NULL;
  }
  GIOStatus encodingSetStatus;
  encodingSetStatus = g_io_channel_set_encoding(toWriteChannel,NULL,NULL);
	if (G_IO_STATUS_NORMAL != encodingSetStatus ){
		g_warning("failed to set to binary mode");
    return NULL;
	}

  return toWriteChannel;
}

EventHeader* getEventHeaderFromWrapper(EventWrapper* eventWrapper){
  guint8 eventType;
  eventType=eventWrapper->eventType;
  EventHeader* eventHeader;
  eventHeader=NULL;

  switch( eventType ){
    case FORMAT_DESCRIPTION_EVENT:{
      eventHeader = ((FormatDescriptionEvent*)eventWrapper->eventPointer)->eventHeader;
      break;
    }
    case TABLE_MAP_EVENT:{
      eventHeader = ((TableMapEvent*)eventWrapper->eventPointer)->eventHeader;
      break;
    }
    case WRITE_ROWS_EVENT:
    case UPDATE_ROWS_EVENT:
    case DELETE_ROWS_EVENT:{
      eventHeader = ((RowEvent*)eventWrapper->eventPointer)->eventHeader;
      break;
    }
    case QUERY_EVENT:{
      eventHeader = ((QueryEvent*)eventWrapper->eventPointer)->eventHeader;
      break;
    }
    case XID_EVENT:{
      eventHeader = ((XidEvent*)eventWrapper->eventPointer)->eventHeader;
      break;
    }
    case GTID_LOG_EVENT:{
      eventHeader = ((GtidEvent*)eventWrapper->eventPointer)->eventHeader;
      break;
    }
    default:
      eventHeader =  NULL;

  }
  return eventHeader;
}

gchar* getRawEventDataFromWrapper(EventWrapper* eventWrapper){
  guint8 eventType;
  eventType=eventWrapper->eventType;
  gchar* rawEventData;
  rawEventData=NULL;

  switch( eventType ){
    case FORMAT_DESCRIPTION_EVENT:{
      rawEventData = ((FormatDescriptionEvent*)eventWrapper->eventPointer)->rawFormatDescriptionEventDataDetail;
      break;
    }
    case TABLE_MAP_EVENT:{
      rawEventData = ((TableMapEvent*)eventWrapper->eventPointer)->rawTableMapEventDataDetail;
      break;
    }
    case WRITE_ROWS_EVENT:
    case UPDATE_ROWS_EVENT:
    case DELETE_ROWS_EVENT:{
      rawEventData = ((RowEvent*)eventWrapper->eventPointer)->rawRowEventDataDetail;
      break;
    }
    case QUERY_EVENT:{
      rawEventData = ((QueryEvent*)eventWrapper->eventPointer)->rawQueryEventDataDetail;
      break;
    }
    case XID_EVENT:{
      rawEventData = ((XidEvent*)eventWrapper->eventPointer)->rawXidEventDataDetail;
      break;
    }
    case GTID_LOG_EVENT:{
      rawEventData = ((GtidEvent*)eventWrapper->eventPointer)->rawGtidEventDataDetail;
      break;
    }
    default:
      rawEventData =  NULL;

  }
  return rawEventData;
}



int printHex(gchar* dataBuffer, guint64 dataBufferLength){
  guint64 i;
  i=0;
  for(;i<dataBufferLength;i++){
    if((i>0) &&(0==(i%16))){
      g_debug("%02x",(guint8)dataBuffer[i]);
    }else{
      g_debug("%02x",(guint8)dataBuffer[i]);
    }
  }
  return 0;
}

int printEventInHex(guint8 eventType, gchar* rawEventHeader, gchar* eventDataDetail, guint64 eventDataDetailLength){

  g_warning("%s header:",Binlog_event_type_name[eventType]);
  printHex(rawEventHeader,EVENT_HEADER_LENGTH);
  g_debug("data:");
  printHex(eventDataDetail,eventDataDetailLength);
  return 0;

}
//
int printEventWrapperInHex(EventWrapper *eventWrapper){
  guint8 eventType;
  eventType=eventWrapper->eventType;
  gchar* rawEventHeader;
  gchar* eventDataDetail;
  guint64 eventDataDetailLength;

  switch ( eventType) {
    case FORMAT_DESCRIPTION_EVENT:{
      rawEventHeader=((FormatDescriptionEvent*)(eventWrapper->eventPointer))->eventHeader->rawEventHeader;
      eventDataDetail=((FormatDescriptionEvent*)(eventWrapper->eventPointer))->rawFormatDescriptionEventDataDetail;
      eventDataDetailLength=getRawEventDataLengthWithChecksum(((FormatDescriptionEvent*)(eventWrapper->eventPointer))->eventHeader);
      break;
    }
    case TABLE_MAP_EVENT:{
      rawEventHeader=((TableMapEvent*)(eventWrapper->eventPointer))->eventHeader->rawEventHeader;
      eventDataDetail=((TableMapEvent*)(eventWrapper->eventPointer))->rawTableMapEventDataDetail;
      eventDataDetailLength=getRawEventDataLengthWithChecksum(((TableMapEvent*)(eventWrapper->eventPointer))->eventHeader);
      break;
    }
    case QUERY_EVENT:{
      rawEventHeader=((QueryEvent*)(eventWrapper->eventPointer))->eventHeader->rawEventHeader;
      eventDataDetail=((QueryEvent*)(eventWrapper->eventPointer))->rawQueryEventDataDetail;
      eventDataDetailLength=getRawEventDataLengthWithChecksum(((QueryEvent*)(eventWrapper->eventPointer))->eventHeader);
      break;
    }
    case XID_EVENT:{
      rawEventHeader=((XidEvent*)(eventWrapper->eventPointer))->eventHeader->rawEventHeader;
      eventDataDetail=((XidEvent*)(eventWrapper->eventPointer))->rawXidEventDataDetail;
      eventDataDetailLength=getRawEventDataLengthWithChecksum(((XidEvent*)(eventWrapper->eventPointer))->eventHeader);
      break;
    }
    case WRITE_ROWS_EVENT:
    case UPDATE_ROWS_EVENT:
    case DELETE_ROWS_EVENT:{
      rawEventHeader=((RowEvent*)(eventWrapper->eventPointer))->eventHeader->rawEventHeader;
      eventDataDetail=((RowEvent*)(eventWrapper->eventPointer))->rawRowEventDataDetail;
      eventDataDetailLength=getRawEventDataLengthWithChecksum(((RowEvent*)(eventWrapper->eventPointer))->eventHeader);
    }
    default :{
      g_warning("skip to print the event %s",Binlog_event_type_name[eventType] );
      return 0;
    }
  }

  printEventInHex(eventType,rawEventHeader,eventDataDetail,eventDataDetailLength);
  return 0;

}

int printRowEventInHex(RowEvent *rowEvent){
  gchar* rawEventHeader;
  rawEventHeader=rowEvent->eventHeader->rawEventHeader;
  gchar* eventDataDetail;
  eventDataDetail=rowEvent->rawRowEventDataDetail;
  guint8 eventType;
  eventType=rowEvent->eventHeader->eventType;
  guint64 eventDataDetailLength;
  eventDataDetailLength=getRawEventDataLengthWithChecksum(rowEvent->eventHeader);

  printEventInHex(eventType,rawEventHeader,eventDataDetail,eventDataDetailLength);

  return 0;

}




int printLeastExecutionUnitEventsInHex(LeastExecutionUnitEvents *leastExecutionUnitEvents){
  TableMapEvent *tableMapEvent;
  tableMapEvent=leastExecutionUnitEvents->tableMapEvent;
  printEventInHex(TABLE_MAP_EVENT,tableMapEvent->eventHeader->rawEventHeader,tableMapEvent->rawTableMapEventDataDetail,getRawEventDataLengthWithChecksum(tableMapEvent->eventHeader));

  GList *rowEventList;
  rowEventList=leastExecutionUnitEvents->rowEventList;
  RowEvent *rowEvent;

  while( NULL != rowEventList ){
    rowEvent=(RowEvent *)rowEventList->data;
    printRowEventInHex(rowEvent);
    rowEventList=rowEventList->next;
  }

  return 0;
}


gboolean isRowEvent(guint8 eventType){
  if ( (eventType == UPDATE_ROWS_EVENT) || (eventType == DELETE_ROWS_EVENT) || (eventType == WRITE_ROWS_EVENT) ){
    return TRUE;
  }
  else
    return FALSE;
}

gboolean isTransactionBeginText(gchar* sqlText){
  if ( 0 == g_ascii_strcasecmp(sqlText, "BEGIN") ){
    return TRUE;
  }
  else
    return FALSE;
}

gchar* getStringAndAdvance(gchar* buffer, guint64 stringLength, gchar* stringValue){
	memcpy(stringValue,buffer,stringLength);
	return buffer+stringLength;
}


gchar* getGuint64AndAdvance(gchar *buffer, guint64 *value ){
	memcpy(value, buffer,sizeof(guint64));
	return buffer+sizeof(guint64);
}

gchar* getGuint48AndAdvance(gchar *buffer, guint64 *value ){
	*value=0;
	guint64 uint48Length=6;
	memcpy(value,buffer,uint48Length);
	return buffer+uint48Length;
}

gchar* getGuint32AndAdvance(gchar *buffer, guint32 *value ){
	memcpy(value, buffer,sizeof(guint32));
	return buffer+sizeof(guint32);
}

gchar* getGuint64AndAdvance4Byte(gchar *buffer, guint64 *value ){
	memcpy(value, buffer,4);
	return buffer+4;
}

gchar* getGuint16AndAdvance(gchar *buffer, guint16 *value ){
	memcpy(value, buffer,sizeof(guint16));
	//*value=GINT_FROM_BE(*value);
	return buffer+sizeof(guint16);
}

gchar* getGuint8AndAdvance(gchar *buffer, guint8 *value ){
	memcpy(value, buffer,sizeof(guint8));
	return buffer+sizeof(guint8);
}



gchar* getPackedIntegerAndAdvance(gchar* buffer, guint64 *value){
	*value=0;
	guint8 lengthIndicator;
	memcpy(&lengthIndicator,buffer,sizeof(guint8));
	if( 251 >= lengthIndicator){
		lengthIndicator=1;

	}
	else if(252 == lengthIndicator){
		lengthIndicator=2;
		buffer +=1;
	}
	else if(253 == lengthIndicator){
		lengthIndicator=3;
		buffer +=1;
	}
	else if(254 == lengthIndicator){
		lengthIndicator=8;
		buffer +=1;
	}


	memcpy(value, buffer,lengthIndicator);

	return buffer+lengthIndicator;


}

guint64 getBitMapLengthByColumnNumber(guint64 columnNumber){
  return (columnNumber+7)/8;
}

gboolean  isBitmapSet(gchar* bitmap, guint64 bitPos)
{

  return ((gchar*)bitmap)[bitPos / 8] & (1 << (bitPos & 7));
}

int parseHeader(gchar *buffer, EventHeader* eventHeader){

	eventHeader->rawEventHeader=buffer;

	guint32 binlogTimestamp ;
	buffer=getGuint32AndAdvance(buffer,&binlogTimestamp);
	eventHeader->binlogTimestamp=binlogTimestamp;

	guint8 eventType;
	buffer=getGuint8AndAdvance(buffer,&eventType);
	eventHeader->eventType=eventType;

	guint32 serverId;
	buffer=getGuint32AndAdvance(buffer,&serverId);
	eventHeader->serverId=serverId;

	guint32 eventLength;
	buffer=getGuint32AndAdvance(buffer,&eventLength);
	eventHeader->eventLength=eventLength;

	guint64 nextEventPos;
        nextEventPos=0;
	buffer=getGuint64AndAdvance4Byte(buffer,&nextEventPos);
	eventHeader->nextEventPos=le64toh(nextEventPos);	

	guint16 flag;
	buffer=getGuint16AndAdvance(buffer,&flag);
	eventHeader->flag=flag;

	return 0;
}

int updateRawEventHeaderByModifyConstructMember(EventHeader *eventHeader){

  gchar* rawEventHeader;
  rawEventHeader=eventHeader->rawEventHeader;
  memcpy(rawEventHeader,&(eventHeader->binlogTimestamp),sizeof(guint32));
  rawEventHeader+=sizeof(guint32);

  memcpy(rawEventHeader,&(eventHeader->eventType),sizeof(guint8));
  rawEventHeader+=sizeof(guint8);

  memcpy(rawEventHeader,&(eventHeader->serverId),sizeof(guint32));
  rawEventHeader+=sizeof(guint32);

  memcpy(rawEventHeader,&(eventHeader->eventLength),sizeof(guint32));
  rawEventHeader+=sizeof(guint32);

  memcpy(rawEventHeader,&(eventHeader->nextEventPos),sizeof(guint32));
  rawEventHeader+=sizeof(guint32);

  memcpy(rawEventHeader,&(eventHeader->flag),sizeof(guint16));
  rawEventHeader+=sizeof(guint16);

  return 0;

}

int setStmtEndFlag(RowEvent *rowEvent){
  guint16 StmtEndFlag = 01U;
  guint16 flag = 0U;
  memcpy(&flag, ((gchar*)(rowEvent->rawRowEventDataDetail))+6, sizeof(guint16));
  flag |= StmtEndFlag;
  //eventHeader->flag |= StmtEndFlag;
  //17 is offset of flag position
  memcpy(((gchar*)(rowEvent->rawRowEventDataDetail))+6, &flag, sizeof(guint16));
  return 0;

}

guint64 modifyAndReturnNextEventPos(EventHeader *eventHeader, guint64 currentPos){
  eventHeader->nextEventPos= eventHeader->eventLength + currentPos ;
  updateRawEventHeaderByModifyConstructMember(eventHeader);
  guint64 nextEventPos;
  nextEventPos = eventHeader->nextEventPos;
  return nextEventPos;
}

gboolean markLastRowEventInStatement(EventWrapper* eventWrapper){

  if( (! eventWrapper) || (! isRowEvent(eventWrapper->eventType))){
    return FALSE;
  }

  gchar* rawRowEventDataDetail=((RowEvent*)(eventWrapper->eventPointer))->rawRowEventDataDetail;
  guint64 rowFlagOffset=6;
  guint16 rowFlag;
  rowFlag=getGuint16AndAdvance(rawRowEventDataDetail+rowFlagOffset,&rowFlag);
  rowFlag = (rowFlag | STMT_END_F);
  memcpy(rawRowEventDataDetail+rowFlagOffset,&rowFlag,sizeof(guint16));
  return TRUE;

}

guint32 decimalBinarySize( guint32 precision,guint32 scale)
 {
   static const int dig2bytes[10]= {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

   guint32 tmp;
   if(scale<precision){
   	tmp=scale;
	scale=precision;
	precision=tmp;
   }

   int intg= scale-precision,
       intg0= intg/9, frac0= precision/9,
       intg0x= intg-intg0*9, frac0x= precision-frac0*9;

   return intg0 * sizeof(guint32) + dig2bytes[intg0x]+
          frac0 * sizeof(guint32) + dig2bytes[frac0x];
 }

 guint32 myTimeBinaryLength(guint32 dec)
 {
   //BAPI_ASSERT(dec <= DATETIME_MAX_DECIMALS);
   return 3 + (dec + 1) / 2;
 }

guint32 uintMax(guint32 bits) {
   //BAPI_ASSERT(static_cast<unsigned int>(bits) <= sizeof(unsigned int) * CHAR_BIT);
   return (((1U << (bits - 1)) - 1) << 1) | 1;
 }


 guint32 myTimestampBinaryLength(guint32 dec)
 {
   //BAPI_ASSERT(dec <= DATETIME_MAX_DECIMALS);
   return 4 + (dec + 1) / 2;
 }

 guint32 myDatetimeBinaryLength(guint32 dec)
 {
   //BAPI_ASSERT(dec <= DATETIME_MAX_DECIMALS);
   return 5 + (dec + 1) / 2;
 }

 guint32
 maxDisplayLengthForField(enum_field_types sqlType, guint32 metadata)
 {
   //BAPI_ASSERT(metadata >> 16 == 0);

   switch (sqlType) {
   case MYSQL_TYPE_NEWDECIMAL:
     return metadata >> 8;

   case MYSQL_TYPE_FLOAT:
     return 12;

   case MYSQL_TYPE_DOUBLE:
     return 22;

   case MYSQL_TYPE_SET:
   case MYSQL_TYPE_ENUM:
       return metadata & 0x00ff;

   case MYSQL_TYPE_STRING:
   {
     guint8 type= metadata >> 8;
     if (type == MYSQL_TYPE_SET || type == MYSQL_TYPE_ENUM)
       return metadata & 0xff;
     else
       return (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0x00ff);
   }

   case MYSQL_TYPE_YEAR:
   case MYSQL_TYPE_TINY:
     return 4;

   case MYSQL_TYPE_SHORT:
     return 6;

   case MYSQL_TYPE_INT24:
     return 9;

   case MYSQL_TYPE_LONG:
     return 11;

   case MYSQL_TYPE_LONGLONG:
     return 20;

   case MYSQL_TYPE_NULL:
     return 0;

   case MYSQL_TYPE_NEWDATE:
     return 3;

   case MYSQL_TYPE_DATE:
   case MYSQL_TYPE_TIME:
   case MYSQL_TYPE_TIME2:
     return 3;

   case MYSQL_TYPE_TIMESTAMP:
   case MYSQL_TYPE_TIMESTAMP2:
     return 4;

   case MYSQL_TYPE_DATETIME:
   case MYSQL_TYPE_DATETIME2:
     return 8;

   case MYSQL_TYPE_BIT:
     /*
       Decode the size of the bit field from the master.
     */
     //BAPI_ASSERT((metadata & 0xff) <= 7);
     return 8 * (metadata >> 8U) + (metadata & 0x00ff);

   case MYSQL_TYPE_VAR_STRING:
   case MYSQL_TYPE_VARCHAR:
     return metadata;

     /*
       The actual length for these types does not really matter since
       they are used to calc_pack_length, which ignores the given
       length for these types.

       Since we want this to be accurate for other uses, we return the
       maximum size in bytes of these BLOBs.
     */

   case MYSQL_TYPE_TINY_BLOB:
     return uintMax(1 * 8);

   case MYSQL_TYPE_MEDIUM_BLOB:
     return uintMax(3 * 8);

   case MYSQL_TYPE_BLOB:
     /*
       For the blob type, Field::real_type() lies and say that all
       blobs are of type MYSQL_TYPE_BLOB. In that case, we have to look
       at the length instead to decide what the max display size is.
      */
     return uintMax(metadata * 8);

   case MYSQL_TYPE_LONG_BLOB:
   case MYSQL_TYPE_GEOMETRY:
   case MYSQL_TYPE_JSON:
     return uintMax(4 * 8);

   default:
     return UINT_MAX;
   }
 }

//need to format the ident
guint64 getMetadataLength(guint8 fieldType, gchar* dataBuffer){
  guint64 metadataLength;
  metadataLength=0;
  guint64 length;
  length=0;

  switch ((fieldType)) {
  case MYSQL_TYPE_NEWDECIMAL:
    metadataLength=2;


    break;
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_FLOAT:
  case MYSQL_TYPE_DOUBLE:
    metadataLength=1;
    break;
  /*
    The cases for SET and ENUM are include for completeness, however
    both are mapped to type MYSQL_TYPE_STRING and their real types
    are encoded in the field metadata.
  */
  case MYSQL_TYPE_SET:
  case MYSQL_TYPE_ENUM:
  case MYSQL_TYPE_STRING:
  {
    metadataLength = 2;
    break;
  }
  case MYSQL_TYPE_YEAR:
  case MYSQL_TYPE_TINY:
    length= 1;
    break;
  case MYSQL_TYPE_SHORT:
    length= 2;
    break;
  case MYSQL_TYPE_INT24:
    length= 3;
    break;
  case MYSQL_TYPE_LONG:
    length= 4;
    break;
  case MYSQL_TYPE_LONGLONG:
    length= 8;
    break;
  case MYSQL_TYPE_NULL:
    length= 0;
    break;
  case MYSQL_TYPE_NEWDATE:
    length= 3;
    break;
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_TIME:
    length= 3;
    break;
  case MYSQL_TYPE_TIME2:
    /*
      The original methods in the server to calculate the binary size of the
      packed numeric time representation is defined in my_time.c, the signature
      being  unsigned int my_time_binary_length(uint)

      The length below needs to be updated if the above method is updated in
      the server
    */
    metadataLength=1;
    break;
  case MYSQL_TYPE_TIMESTAMP:
    length= 4;
    break;
  case MYSQL_TYPE_TIMESTAMP2:
    /*
      The original methods in the server to calculate the binary size of the
      packed numeric time representation is defined in time.c, the signature
      being  unsigned int my_timestamp_binary_length(uint)

      The length below needs to be updated if the above method is updated in
      the server
    */
    metadataLength=1;
    break;
  case MYSQL_TYPE_DATETIME:
    length= 8;
    break;
  case MYSQL_TYPE_DATETIME2:
    /*
      The original methods in the server to calculate the binary size of the
      packed numeric time representation is defined in time.c, the signature
      being  unsigned int my_datetime_binary_length(uint)

      The length below needs to be updated if the above method is updated in
      the server
    */
    metadataLength=1;
    break;
  case MYSQL_TYPE_BIT:
  {
    /*
      Decode the size of the bit field from the master.
        from_len is the length in bytes from the master
        from_bit_len is the number of extra bits stored in the master record
      If from_bit_len is not 0, add 1 to the length to account for accurate
      number of bytes needed.
    */
    metadataLength=2;
    break;
  }
  case MYSQL_TYPE_VARCHAR:
  {
    metadataLength=2;
    break;
  }
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_JSON:
  {
    /*
      Compute the length of the data. We cannot use get_length() here
      since it is dependent on the specific table (and also checks the
      packlength using the internal 'table' pointer) and replication
      is using a fixed format for storing data in the binlog.
    */
    metadataLength=1;
    break;
  }
  }
  return metadataLength;



}

enum_field_types real_type(guint8 fieldType, guint32 metadata){

  switch(fieldType){
    case MYSQL_TYPE_STRING:
    {
      guint8 type= metadata >> 8;
      return type;
    }
  }
  return fieldType;
}

guint32 calcFieldSize(guint8 fieldType, const gchar *dataBuffer,
                         guint32 metadata)
{
  guint32 length= 0;


  //fieldType = real_type(fieldType, metadata);
  switch ((fieldType)) {
  case MYSQL_TYPE_NEWDECIMAL:
    length= decimalBinarySize(metadata >> 8,
                                metadata & 0xff);


    break;
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_FLOAT:
  case MYSQL_TYPE_DOUBLE:
    length= metadata;
    break;
  /*
    The cases for SET and ENUM are include for completeness, however
    both are mapped to type MYSQL_TYPE_STRING and their real types
    are encoded in the field metadata.
  */
  case MYSQL_TYPE_SET:
  case MYSQL_TYPE_ENUM:
  case MYSQL_TYPE_STRING:
  {
    guint8 type= metadata >> 8U;
    if ((type == MYSQL_TYPE_SET) || (type == MYSQL_TYPE_ENUM)){
      length= metadata & 0x00ff;
    }
    else
    {
      /*
        We are reading the actual size from the dataBuffer record
        because this field has the actual lengh stored in the first
        one or two bytes.
      */
      length= maxDisplayLengthForField(MYSQL_TYPE_STRING, metadata) > 255 ? 2 : 1;

      if (length == 1){
        length+= *dataBuffer;

      }
      else
      {
        guint32 temp= 0;
        memcpy(&temp, dataBuffer, 2);
        length= length + GINT_FROM_LE(temp);

      }
    }
    break;
  }
  case MYSQL_TYPE_YEAR:
  case MYSQL_TYPE_TINY:
    length= 1;
    break;
  case MYSQL_TYPE_SHORT:
    length= 2;
    break;
  case MYSQL_TYPE_INT24:
    length= 3;
    break;
  case MYSQL_TYPE_LONG:
    length= 4;
    break;
  case MYSQL_TYPE_LONGLONG:
    length= 8;
    break;
  case MYSQL_TYPE_NULL:
    length= 0;
    break;
  case MYSQL_TYPE_NEWDATE:
    length= 3;
    break;
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_TIME:
    length= 3;
    break;
  case MYSQL_TYPE_TIME2:
    /*
      The original methods in the server to calculate the binary size of the
      packed numeric time representation is defined in my_time.c, the signature
      being  unsigned int my_time_binary_length(uint)

      The length below needs to be updated if the above method is updated in
      the server
    */
    length= myTimeBinaryLength(metadata);
    break;
  case MYSQL_TYPE_TIMESTAMP:
    length= 4;
    break;
  case MYSQL_TYPE_TIMESTAMP2:
    /*
      The original methods in the server to calculate the binary size of the
      packed numeric time representation is defined in time.c, the signature
      being  unsigned int my_timestamp_binary_length(uint)

      The length below needs to be updated if the above method is updated in
      the server
    */
    length= myTimestampBinaryLength(metadata);
    break;
  case MYSQL_TYPE_DATETIME:
    length= 8;
    break;
  case MYSQL_TYPE_DATETIME2:
    /*
      The original methods in the server to calculate the binary size of the
      packed numeric time representation is defined in time.c, the signature
      being  unsigned int my_datetime_binary_length(uint)

      The length below needs to be updated if the above method is updated in
      the server
    */
    length= myDatetimeBinaryLength(metadata);
    break;
  case MYSQL_TYPE_BIT:
  {
    /*
      Decode the size of the bit field from the master.
        from_len is the length in bytes from the master
        from_bit_len is the number of extra bits stored in the master record
      If from_bit_len is not 0, add 1 to the length to account for accurate
      number of bytes needed.
    */
    unsigned int from_len= (metadata >> 8U) & 0x00ff;
    unsigned int from_bit_len= metadata & 0x00ff;
    //BAPI_ASSERT(from_bit_len <= 7);
    length= from_len + ((from_bit_len > 0) ? 1 : 0);
    break;
  }
  case MYSQL_TYPE_VARCHAR:
  {
    length= metadata > 255 ? 2 : 1;
    if (length == 1)
      length+= (guint8) *dataBuffer;
    else
    {
      guint32 temp= 0;
      memcpy(&temp, dataBuffer, 2);
      length= length + GINT_FROM_LE(temp);
    }
    break;
  }
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_JSON:
  {
    /*
      Compute the length of the data. We cannot use get_length() here
      since it is dependent on the specific table (and also checks the
      packlength using the internal 'table' pointer) and replication
      is using a fixed format for storing data in the binlog.
    */
    switch (metadata) {
    case 1:
      length= *dataBuffer;
      break;
    case 2:
      memcpy(&length, dataBuffer, 2);
      length= GINT_FROM_LE(length);
      break;
    case 3:
      memcpy(&length, dataBuffer, 3);
      length= GINT_FROM_LE(length);
      break;
    case 4:
      memcpy(&length, dataBuffer, 4);
      length= GINT_FROM_LE(length);
      break;
    default:
      //BAPI_ASSERT(0);		// Should not come here
      break;
    }

    length+= metadata;
    break;
  }
  default:
    length= UINT_MAX;
  }
  return length;
}


int initQueryEvent(QueryEvent *queryEvent, EventHeader *eventHeader, gchar *rawQueryEventDataDetail){
  queryEvent->eventHeader=eventHeader;
  queryEvent->rawQueryEventDataDetail=rawQueryEventDataDetail;
  return 0;
}

int initRowEvent(RowEvent *rowEvent, EventHeader *eventHeader, gchar *rawRowEventDataDetail ){
	rowEvent->eventHeader=eventHeader;
	rowEvent->rawRowEventDataDetail=rawRowEventDataDetail;
	return 0;
}

int initFormatDescriptionEvent(FormatDescriptionEvent *formatDescriptionEvent,EventHeader *eventHeader,gchar *rawFormatDescriptionEventDataDetail){
	formatDescriptionEvent->eventHeader=eventHeader;
	formatDescriptionEvent->rawFormatDescriptionEventDataDetail=rawFormatDescriptionEventDataDetail;
	return 0;
}

int initTableMapEvent(TableMapEvent *tableMapEvent,EventHeader *eventHeader,gchar *rawTableMapEventDataDetail){

	tableMapEvent->rawTableMapEventDataDetail=rawTableMapEventDataDetail;
	GByteArray *columnTypeArray;
	columnTypeArray = g_byte_array_new();
	tableMapEvent->columnTypeArray=columnTypeArray;
	tableMapEvent->eventHeader=eventHeader;
	return 0;
}

int initXidEvent(XidEvent* xidEvent, EventHeader *eventHeader, gchar *rawXidEventDataDetail){
  xidEvent->rawXidEventDataDetail=rawXidEventDataDetail;
  xidEvent->eventHeader=eventHeader;
  return 0;
}

int initGtidEvent(GtidEvent* gtidEvent, EventHeader *eventHeader, gchar *rawGtidEventDataDetail){
  gtidEvent->rawGtidEventDataDetail=rawGtidEventDataDetail;
  gtidEvent->eventHeader=eventHeader;
  return 0;
}

int parseQueryLogEvent(gchar* dataBuffer, QueryEvent* queryEvent){
  gchar* dataBufferStart;
  dataBufferStart=dataBuffer;
  guint32 threadId;
  dataBuffer=getGuint32AndAdvance(dataBuffer,&threadId);

  guint32 seconds;
  dataBuffer=getGuint32AndAdvance(dataBuffer,&seconds);

  guint8 databaseNameLength;
  dataBuffer=getGuint8AndAdvance(dataBuffer,&databaseNameLength);
  queryEvent->databaseNameLength=databaseNameLength;

  guint16 errorCode;
  dataBuffer=getGuint16AndAdvance(dataBuffer,&errorCode);

  guint16 statusLength;
  dataBuffer=getGuint16AndAdvance(dataBuffer,&statusLength);

  gchar* statusBlock;
  statusBlock=g_new0(gchar,statusLength);
  dataBuffer=getStringAndAdvance(dataBuffer,statusLength,statusBlock);

  gchar* databaseName;
  databaseName=g_new0(gchar,databaseNameLength+1);
  dataBuffer=getStringAndAdvance(dataBuffer,databaseNameLength+1,databaseName);
  queryEvent->databaseName=databaseName;

  gchar* sqlText;
  guint16 sqlTextLength;
  sqlTextLength=queryEvent->eventHeader->eventLength - EVENT_HEADER_LENGTH-(dataBuffer -dataBufferStart)-CHECKSUM_LENGTH;
  queryEvent->sqlTextLength=sqlTextLength;

  sqlText=g_new0(gchar,sqlTextLength+1);
  dataBuffer=getStringAndAdvance(dataBuffer,sqlTextLength,sqlText);
  queryEvent->sqlText=sqlText;

  return 0;

}

int parseXidEvent(gchar* dataBuffer, XidEvent* xidEvent){
  guint64 xid;
  dataBuffer=getGuint64AndAdvance(dataBuffer,&xid);
  xidEvent->xid=xid;
  return 0;
}

int parseGtidEvent(gchar* dataBuffer, GtidEvent* gtidEvent){
  guint64 unuseByteLength=1;
  guint8 unusedByte;
  dataBuffer=getStringAndAdvance(dataBuffer,unuseByteLength,&unusedByte);
  gchar* uuid=g_new0(gchar,16+1);
  dataBuffer=getStringAndAdvance(dataBuffer,16,uuid);
  uuid[16]='\0';
  gtidEvent->uuid=uuid;

  guint64 seqNo;
  dataBuffer=getGuint64AndAdvance(dataBuffer,&seqNo);
  gtidEvent->seqNo=seqNo;

  return 0;

}

int parseTableMapEventData(gchar* dataBuffer,  TableMapEvent* tableMapEvent){

	guint64 tableId;
	dataBuffer=getGuint48AndAdvance(dataBuffer,&tableId);
	tableMapEvent->tableId=tableId;

	guint16 skipGuint16;
	dataBuffer=getGuint16AndAdvance(dataBuffer,&skipGuint16);


	guint8 databaseNameLength;
	dataBuffer=getGuint8AndAdvance(dataBuffer,&databaseNameLength);
  tableMapEvent->databaseNameLength=databaseNameLength;


	gchar* databaseName;
	databaseName=g_new0(gchar,databaseNameLength+1);
	dataBuffer=getStringAndAdvance(dataBuffer,databaseNameLength+1,databaseName);
	tableMapEvent->databaseName=databaseName;

	guint8 tableNameLength;
	dataBuffer=getGuint8AndAdvance(dataBuffer,&tableNameLength);
  tableMapEvent->tableNameLength=tableNameLength;

	gchar* tableName;
	tableName=g_new0(gchar,tableNameLength+1);
	dataBuffer=getStringAndAdvance(dataBuffer,tableNameLength+1,tableName);
	tableMapEvent->tableName=tableName;

	guint64 columnNumber;
	dataBuffer=getPackedIntegerAndAdvance(dataBuffer,&columnNumber);
	tableMapEvent->columnNumber=columnNumber;

	gchar* columnTypeArray;
	columnTypeArray=g_new0(gchar,columnNumber);
	int i=0;
	while(i<columnNumber){
	  dataBuffer=getStringAndAdvance(dataBuffer,1,columnTypeArray);
	  g_byte_array_append(tableMapEvent->columnTypeArray,columnTypeArray,1/*column type length*/);
	  i++;
	}

  guint16 *columnMetadataArray;
  columnMetadataArray = (guint16 *)malloc(sizeof(guint16)*columnNumber);
  tableMapEvent->columnMetadataArray=columnMetadataArray;

  guint64 metadataBlockSize=0;
  dataBuffer=getPackedIntegerAndAdvance(dataBuffer,&metadataBlockSize);

  int columnIndex=0;
  guint64 metadataLength=0;
  guint64 metadata;
  for(;columnIndex<tableMapEvent->columnTypeArray->len;columnIndex++){
    metadataLength=getMetadataLength(tableMapEvent->columnTypeArray->data[columnIndex],dataBuffer);
    metadata=0;
    if(0 == metadataLength){
      tableMapEvent->columnMetadataArray[columnIndex]=0;
    }else if( 1 == metadataLength ){
      dataBuffer=getGuint8AndAdvance(dataBuffer,&metadata);
      tableMapEvent->columnMetadataArray[columnIndex]=metadata;
    }else if( 2 == metadataLength ){
      dataBuffer=getGuint16AndAdvance(dataBuffer,&metadata);
      if(MYSQL_TYPE_STRING==tableMapEvent->columnTypeArray->data[columnIndex]){
        metadata=htobe16(metadata);
      }
      tableMapEvent->columnMetadataArray[columnIndex]=metadata;
    }
  }



	return 0;
}

int parseRowEventData(gchar* dataBuffer, RowEvent *rowEvent, EventHeader* eventHeader){
	return 0;
}

int appendToAllEventList(GList **allEventsList, EventHeader *eventHeader,gpointer someEvent){
  EventWrapper *eventWrapper;
  eventWrapper = g_new0(EventWrapper,1);
  eventWrapper->eventType = eventHeader->eventType;
  eventWrapper->eventPointer=someEvent;
  *allEventsList=g_list_prepend(*allEventsList,( gpointer )eventWrapper);

  return 0;
}

/*
guint64 getRawEventDataLengthWithChecksum(EventHeader *eventHeader){
  guint64 rawEventDataLength;
  rawEventDataLength=eventHeader->eventLength - EVENT_HEADER_LENGTH; // - CHECKSUM_LENGTH
  return rawEventDataLength;
}

*/

EventHeader* deepCopyEventHeader( EventHeader *originalEventHeader ){
  EventHeader* targetEventHeader;
  targetEventHeader = g_new0(EventHeader, 1);
  targetEventHeader->binlogTimestamp=originalEventHeader->binlogTimestamp;
  targetEventHeader->eventType=originalEventHeader->eventType;
  targetEventHeader->serverId=originalEventHeader->serverId;
  targetEventHeader->eventLength=originalEventHeader->eventLength;
  targetEventHeader->nextEventPos=originalEventHeader->nextEventPos;
  targetEventHeader->flag=originalEventHeader->flag;

  gchar *targetRawEventHeader;
  targetRawEventHeader=g_new0(gchar, EVENT_HEADER_LENGTH);
  memcpy(targetRawEventHeader,originalEventHeader->rawEventHeader,EVENT_HEADER_LENGTH);
  targetEventHeader->rawEventHeader=targetRawEventHeader;

  return targetEventHeader;
}


EventWrapper* deepCopyEventWrapper( EventWrapper *originalEventWrapper ){
  EventWrapper* targetEventWrapper;
  targetEventWrapper = g_new0(EventWrapper, 1);
  targetEventWrapper->eventType= originalEventWrapper->eventType;
  switch (targetEventWrapper->eventType) {
    case FORMAT_DESCRIPTION_EVENT:{
      EventHeader *targetEventHeader = deepCopyEventHeader(((FormatDescriptionEvent*)originalEventWrapper->eventPointer)->eventHeader);
      FormatDescriptionEvent *formatDescriptionEvent;
      formatDescriptionEvent=g_new0(FormatDescriptionEvent,1);
      targetEventWrapper->eventPointer=formatDescriptionEvent;

      ((FormatDescriptionEvent*)targetEventWrapper->eventPointer)->eventHeader=targetEventHeader;
      gchar *targetRawFormatDescriptionEventDataDetail;
      targetRawFormatDescriptionEventDataDetail= g_new0(gchar,getRawEventDataLengthWithChecksum(targetEventHeader));
      memcpy(targetRawFormatDescriptionEventDataDetail,((FormatDescriptionEvent*)originalEventWrapper->eventPointer)->rawFormatDescriptionEventDataDetail,getRawEventDataLengthWithChecksum(targetEventHeader));
      ((FormatDescriptionEvent*)targetEventWrapper->eventPointer)->rawFormatDescriptionEventDataDetail=targetRawFormatDescriptionEventDataDetail;
      return targetEventWrapper;

    }
    case TABLE_MAP_EVENT:{
      EventHeader *targetEventHeader = deepCopyEventHeader(((TableMapEvent*)originalEventWrapper->eventPointer)->eventHeader);
      TableMapEvent *tableMapEvent;
      tableMapEvent=g_new0(TableMapEvent,1);
      targetEventWrapper->eventPointer=tableMapEvent;

      ((TableMapEvent*)targetEventWrapper->eventPointer)->eventHeader=targetEventHeader;

      gchar *targetRawTableMapEventDataDetail;
      targetRawTableMapEventDataDetail=g_new0(gchar,getRawEventDataLengthWithChecksum(targetEventHeader));
      memcpy(targetRawTableMapEventDataDetail,((TableMapEvent*)originalEventWrapper->eventPointer)->rawTableMapEventDataDetail,getRawEventDataLengthWithChecksum(targetEventHeader));
      ((TableMapEvent*)targetEventWrapper->eventPointer)->rawTableMapEventDataDetail=targetRawTableMapEventDataDetail;

      ((TableMapEvent*)targetEventWrapper->eventPointer)->databaseNameLength=((TableMapEvent*)originalEventWrapper->eventPointer)->databaseNameLength;

      gchar* targetDatabaseName;
      targetDatabaseName=g_new0(gchar,((TableMapEvent*)originalEventWrapper->eventPointer)->databaseNameLength + 1);
      memcpy(targetDatabaseName,((TableMapEvent*)originalEventWrapper->eventPointer)->databaseName, ((TableMapEvent*)originalEventWrapper->eventPointer)->databaseNameLength);
      ((TableMapEvent*)targetEventWrapper->eventPointer)->databaseName=targetDatabaseName;

      ((TableMapEvent*)targetEventWrapper->eventPointer)->tableNameLength=((TableMapEvent*)originalEventWrapper->eventPointer)->tableNameLength;

      gchar* targetTableName;
      targetTableName=g_new0(gchar,((TableMapEvent*)originalEventWrapper->eventPointer)->tableNameLength + 1);
      memcpy(targetTableName,((TableMapEvent*)originalEventWrapper->eventPointer)->tableName, ((TableMapEvent*)originalEventWrapper->eventPointer)->tableNameLength);
      ((TableMapEvent*)targetEventWrapper->eventPointer)->tableName=targetTableName;

      ((TableMapEvent*)targetEventWrapper->eventPointer)->tableId=((TableMapEvent*)originalEventWrapper->eventPointer)->tableId;

      ((TableMapEvent*)targetEventWrapper->eventPointer)->columnNumber=((TableMapEvent*)originalEventWrapper->eventPointer)->columnNumber;

      GByteArray *targetColumnTypeArray;
      targetColumnTypeArray=g_byte_array_new();
      //int columnIndex=0;
      //while((columnIndex < ((TableMapEvent*)originalEventWrapper->eventPointer)->columnTypeArray->len) ){
      g_byte_array_append(targetColumnTypeArray,((TableMapEvent*)originalEventWrapper->eventPointer)->columnTypeArray->data, ((TableMapEvent*)originalEventWrapper->eventPointer)->columnTypeArray->len );
	//columnIndex++;

      //}
      //targetColumnTypeArray=g_byte_array_new_take(((TableMapEvent*)originalEventWrapper->eventPointer)->columnTypeArray->data, ((TableMapEvent*)originalEventWrapper->eventPointer)->columnTypeArray->len);
      ((TableMapEvent*)targetEventWrapper->eventPointer)->columnTypeArray=targetColumnTypeArray;

      //guint64 metadataBlockSize=0;
      ((TableMapEvent*)targetEventWrapper->eventPointer)->metadataBlockSize=((TableMapEvent*)originalEventWrapper->eventPointer)->metadataBlockSize;

      guint16 *columnMetadataArray;
      int indexColumnMetadataArray=0;
      columnMetadataArray=malloc(sizeof(guint16)* ((TableMapEvent*)originalEventWrapper->eventPointer)->columnNumber );
      for(;indexColumnMetadataArray<((TableMapEvent*)originalEventWrapper->eventPointer)->columnNumber;indexColumnMetadataArray++){
        columnMetadataArray[indexColumnMetadataArray]=((TableMapEvent*)originalEventWrapper->eventPointer)->columnMetadataArray[indexColumnMetadataArray];
      }

      //
      //memcpy(columnMetadataArray,((TableMapEvent*)originalEventWrapper->eventPointer)->columnMetadataArray,sizeof(guint16)* ((TableMapEvent*)originalEventWrapper->eventPointer)->metadataBlockSize );

      ((TableMapEvent*)targetEventWrapper->eventPointer)->columnMetadataArray=columnMetadataArray;


      return targetEventWrapper;
    }
    case WRITE_ROWS_EVENT:
    case UPDATE_ROWS_EVENT:
    case DELETE_ROWS_EVENT:{
      EventHeader *targetEventHeader = deepCopyEventHeader(((RowEvent*)originalEventWrapper->eventPointer)->eventHeader);
      targetEventWrapper->eventType=((RowEvent*)originalEventWrapper->eventPointer)->eventHeader->eventType;
      RowEvent *rowEvent;
      rowEvent=g_new0(RowEvent,1);
      targetEventWrapper->eventPointer=rowEvent;
      ((RowEvent*)targetEventWrapper->eventPointer)->eventHeader=targetEventHeader;

      gchar *targetRawRowEventDataDetail;
      targetRawRowEventDataDetail=g_new0(gchar,getRawEventDataLengthWithChecksum(targetEventHeader));
      memcpy(targetRawRowEventDataDetail,((RowEvent*)originalEventWrapper->eventPointer)->rawRowEventDataDetail,getRawEventDataLengthWithChecksum(targetEventHeader));
      ((RowEvent*)targetEventWrapper->eventPointer)->rawRowEventDataDetail=targetRawRowEventDataDetail;
      return targetEventWrapper;

    }
    case QUERY_EVENT:{
      EventHeader *targetEventHeader = deepCopyEventHeader(((QueryEvent*)originalEventWrapper->eventPointer)->eventHeader);
      QueryEvent *queryEvent;
      queryEvent=g_new0(QueryEvent,1);
      targetEventWrapper->eventPointer=queryEvent;

      ((QueryEvent*)(targetEventWrapper->eventPointer))->eventHeader=targetEventHeader;

      gchar *targetRawQueryEventDataDetail;
      targetRawQueryEventDataDetail=g_new0(gchar,getRawEventDataLengthWithChecksum(targetEventHeader));
      memcpy(targetRawQueryEventDataDetail,((QueryEvent*)originalEventWrapper->eventPointer)->rawQueryEventDataDetail,getRawEventDataLengthWithChecksum(targetEventHeader));
      ((QueryEvent*)targetEventWrapper->eventPointer)->rawQueryEventDataDetail=targetRawQueryEventDataDetail;

      gchar* targetDatabaseName;
      targetDatabaseName=g_new0(gchar,((QueryEvent*)originalEventWrapper->eventPointer)->databaseNameLength + 1);
      memcpy(targetDatabaseName,((QueryEvent*)originalEventWrapper->eventPointer)->databaseName, ((QueryEvent*)originalEventWrapper->eventPointer)->databaseNameLength);
      ((QueryEvent*)targetEventWrapper->eventPointer)->databaseName=targetDatabaseName;

      gchar* targetSqlText;
      targetSqlText=g_new0(gchar,((QueryEvent*)originalEventWrapper->eventPointer)->sqlTextLength + 1);
      memcpy(targetDatabaseName,((QueryEvent*)originalEventWrapper->eventPointer)->sqlText, ((QueryEvent*)originalEventWrapper->eventPointer)->sqlTextLength);
      ((QueryEvent*)targetEventWrapper->eventPointer)->sqlText=targetSqlText;

      return targetEventWrapper;

    }
    case XID_EVENT:{
      EventHeader *targetEventHeader = deepCopyEventHeader(((XidEvent*)originalEventWrapper->eventPointer)->eventHeader);
      XidEvent *xidEvent;
      xidEvent=g_new0(XidEvent,1);
      targetEventWrapper->eventPointer=xidEvent;
      ((XidEvent*)targetEventWrapper->eventPointer)->eventHeader=targetEventHeader;

      gchar *targetRawXidEventDataDetail;
      targetRawXidEventDataDetail=g_new0(gchar,getRawEventDataLengthWithChecksum(targetEventHeader));
      memcpy(targetRawXidEventDataDetail,((XidEvent*)originalEventWrapper->eventPointer)->rawXidEventDataDetail,getRawEventDataLengthWithChecksum(targetEventHeader));
      ((XidEvent*)targetEventWrapper->eventPointer)->rawXidEventDataDetail=targetRawXidEventDataDetail;

      ((XidEvent*)targetEventWrapper->eventPointer)->xid=((XidEvent*)originalEventWrapper->eventPointer)->xid;

      return targetEventWrapper;

    }
    default:{
      return NULL;
    }

  }

}


//guint32 calculateCrc32()


//int constructBinlogFromQueue(){}

gboolean isLeastExecutionUnitShouldKeep(LeastExecutionUnitEvents *leastExecutionUnitEvents){
  gchar* tableName=leastExecutionUnitEvents->tableMapEvent->tableName;
  gchar* databaseName=leastExecutionUnitEvents->tableMapEvent->databaseName;
  guint8 originalRowEventType=leastExecutionUnitEvents->originalRowEventType;
  guint32 binlogTimestamp=leastExecutionUnitEvents->tableMapEvent->eventHeader->binlogTimestamp;
  g_debug("table name is %s",tableName);
  if( !isTableShouldApply(tableName) ){
    return FALSE;
  }
  if( !isDatabaseShouldApply(databaseName) ){
    return FALSE;
  }
  if( !isEventTypeShouldApply(originalRowEventType) ){
    return FALSE;
  }
  /*
  if( !isInDateTimeRange(binlogTimestamp) ){
    return FALSE;
  }
  */
  return TRUE;
}



int constructLeastExecutionUnitFromAllEventsList(const GList *allEventsList, GList **allLeastExecutionUnitList ){
  EventWrapper* eventWrapper;
  LeastExecutionUnitEvents *leastExecutionUnitEvents;
  leastExecutionUnitEvents=NULL;
  guint64 leastExecutionUnitEventsIndicator;
  leastExecutionUnitEventsIndicator=0;
  TableMapEvent * tableMapEventPointer = NULL;
	if ( !allEventsList ){
		return 0;
	}
	//GArray* allLeastExecutionUnitArray = g_array_sized_new(FALSE, FALSE, sizeof(LeastExecutionUnitEvents), g_list_length(allEventsList)/2);
  while(NULL != allEventsList){

     eventWrapper=(EventWrapper*)(allEventsList->data);

     //begin one tableMapEvent with serveral rowEvents

       if( TABLE_MAP_EVENT == eventWrapper->eventType ){
         tableMapEventPointer = (TableMapEvent *)eventWrapper->eventPointer;
         allEventsList=allEventsList->next;
         continue;
       }else if( isRowEvent(eventWrapper->eventType ) ){
         leastExecutionUnitEvents = g_new0(LeastExecutionUnitEvents,1);
         leastExecutionUnitEvents->tableMapEvent = tableMapEventPointer;
         setStmtEndFlag((RowEvent *)eventWrapper->eventPointer);
         leastExecutionUnitEvents->rowEventList=g_list_append(leastExecutionUnitEvents->rowEventList, (RowEvent *)eventWrapper->eventPointer);
				 leastExecutionUnitEvents->originalRowEventType=eventWrapper->eventType;
         g_debug("%s added to leastExecutionUnitEvents \n",Binlog_event_type_name[eventWrapper->eventType]);
         //if(  (NULL != allEventsList->next) && ( isRowEvent(((EventWrapper*)(allEventsList->next->data))->eventType) ) ){
        //   int i=1;
         //}
         if((TRUE == isLeastExecutionUnitShouldKeep(leastExecutionUnitEvents) )){
           //allLeastExecutionUnitArray = g_array_append_val(allLeastExecutionUnitArray, leastExecutionUnitEvents);
					*allLeastExecutionUnitList=g_list_prepend(*allLeastExecutionUnitList,leastExecutionUnitEvents);
         }


       }else{
          g_debug("skip the event %s\n", Binlog_event_type_name[eventWrapper->eventType]);
       }
    allEventsList=allEventsList->next;
  }
	
	//guint allLeastExecutionUnitArrayIndex = g_array_get_element_size(allLeastExecutionUnitArray);
	//while (allLeastExecutionUnitArrayIndex >0 ){
	//	*allLeastExecutionUnitList=g_list_prepend(*allLeastExecutionUnitList, &allLeastExecutionUnitArray[allLeastExecutionUnitArrayIndex - 1]);
	//	allLeastExecutionUnitArrayIndex--;	
	//}
	
	*allLeastExecutionUnitList = g_list_reverse(*allLeastExecutionUnitList);
  g_debug("%d events in allLeastExecutionUnitList ",g_list_length(*allLeastExecutionUnitList ));
  return 0;

}


int exchangeBeforeRowImageWithAfterRowImage(gchar* beforeImageStart, gchar* beforeImageEnd, gchar* afterImageStart, gchar* afterImageEnd){
  gchar *tmpBeforeImage;
  tmpBeforeImage=g_new0(gchar,beforeImageEnd-beforeImageStart+1);
  /*
  gchar* tmpAfterImage;
  tmpAfterImage=g_new0(gchar,afterImageEnd-afterImageStart+1);
  */
  memcpy(tmpBeforeImage,beforeImageStart,beforeImageEnd-beforeImageStart+1);
  /*
  memcpy(tmpAfterImage,afterImageStart,afterImageEnd-afterImageStart+1);
  memcpy(beforeImageStart,tmpAfterImage,afterImageEnd-afterImageStart+1);
  memcpy(afterImageStart,tmpBeforeImage,beforeImageEnd-beforeImageStart+1);
 */


  memmove(beforeImageStart,afterImageStart,afterImageEnd - afterImageStart +1);
  memcpy(beforeImageStart+(afterImageEnd - afterImageStart+1),tmpBeforeImage,beforeImageEnd-beforeImageStart+1);

  return 0;
}



int reverseLeastExecutionUnitEventsForUpdateRowEvent(LeastExecutionUnitEvents *leastExecutionUnitEventsForUpdateRowEvent){


  GList * rowEventList = leastExecutionUnitEventsForUpdateRowEvent->rowEventList;

  RowEvent * rowEvent;
  while( NULL != rowEventList ){
    guint64 offsetBeforeNullBitMap=0;
    rowEvent=rowEventList->data;
    gchar* rawRowEventDataDetail = rowEvent->rawRowEventDataDetail;


    guint64 tableId;
    rawRowEventDataDetail=getGuint48AndAdvance(rawRowEventDataDetail,&tableId);
    offsetBeforeNullBitMap+=8;

    guint32 skipUnused;
    rawRowEventDataDetail=getGuint32AndAdvance(rawRowEventDataDetail,&skipUnused);
    offsetBeforeNullBitMap+=4;

    guint64 usedColumns;
    rawRowEventDataDetail=getPackedIntegerAndAdvance(rawRowEventDataDetail,&usedColumns);
    gchar *beforeImageColumn=g_new0(gchar,getBitMapLengthByColumnNumber(usedColumns));
    rawRowEventDataDetail=getStringAndAdvance(rawRowEventDataDetail,getBitMapLengthByColumnNumber(usedColumns),beforeImageColumn);
    offsetBeforeNullBitMap+=getBitMapLengthByColumnNumber(usedColumns);

    gchar *afterImageColumn=g_new0(gchar,getBitMapLengthByColumnNumber(usedColumns));
    rawRowEventDataDetail=getStringAndAdvance(rawRowEventDataDetail,getBitMapLengthByColumnNumber(usedColumns),afterImageColumn);
    offsetBeforeNullBitMap+=getBitMapLengthByColumnNumber(usedColumns);

    //gchar* nullBitmap=gg_new0(gchar,getBitMapLengthByColumnNumber(usedColumns));
    //rawRowEventDataDetail=getStringAndAdvance(rawRowEventDataDetail,getBitMapLengthByColumnNumber(usedColumns),nullBitmap);

    gchar* eventLevelRowImageStart= rawRowEventDataDetail;
    gchar* eventLevelRowImageEnd=rawRowEventDataDetail + rowEvent->eventHeader->eventLength -EVENT_HEADER_LENGTH - CHECKSUM_LENGTH -offsetBeforeNullBitMap;


    gchar* singleRowLevelBeforeImageStart;
    gchar* singleRowLevelBeforeImageEnd;

    gchar* singleRowLevelAfterImageStart;
    gchar* singleRowLevelAfterImageEnd;

    singleRowLevelBeforeImageStart=eventLevelRowImageStart;

    while(rawRowEventDataDetail < eventLevelRowImageEnd){
      GByteArray * columnTypeArray= leastExecutionUnitEventsForUpdateRowEvent->tableMapEvent->columnTypeArray;
      guint32 fieldIndex;
      fieldIndex=0;
      guint32 fieldSizePlusLengthBytes;
      gchar* nullBitmap=g_new0(gchar,getBitMapLengthByColumnNumber(usedColumns));

      //BI
      singleRowLevelBeforeImageStart=rawRowEventDataDetail;
      singleRowLevelBeforeImageEnd=singleRowLevelBeforeImageStart;
      rawRowEventDataDetail=getStringAndAdvance(rawRowEventDataDetail,getBitMapLengthByColumnNumber(usedColumns),nullBitmap);
      singleRowLevelBeforeImageEnd+=getBitMapLengthByColumnNumber(usedColumns);
      for(fieldIndex=0;fieldIndex<columnTypeArray->len;fieldIndex++){
        if( isBitmapSet(nullBitmap,fieldIndex) ){
          continue;
        }
        fieldSizePlusLengthBytes=calcFieldSize(columnTypeArray->data[fieldIndex],rawRowEventDataDetail,leastExecutionUnitEventsForUpdateRowEvent->tableMapEvent->columnMetadataArray[fieldIndex]);
	g_warning("BI fieldIndex=%d,fieldSizePlusLengthBytes=%d",fieldIndex,fieldSizePlusLengthBytes);
        singleRowLevelBeforeImageEnd+=fieldSizePlusLengthBytes;
        rawRowEventDataDetail+=fieldSizePlusLengthBytes;
      }
      singleRowLevelBeforeImageEnd--;

      //AI
      singleRowLevelAfterImageStart=rawRowEventDataDetail;
      singleRowLevelAfterImageEnd=singleRowLevelAfterImageStart;
      rawRowEventDataDetail=getStringAndAdvance(rawRowEventDataDetail,getBitMapLengthByColumnNumber(usedColumns),nullBitmap);
      singleRowLevelAfterImageEnd+=getBitMapLengthByColumnNumber(usedColumns);
      for(fieldIndex=0;fieldIndex<columnTypeArray->len;fieldIndex++){
        if( isBitmapSet(nullBitmap,fieldIndex) ){
          continue;
        }
        fieldSizePlusLengthBytes=calcFieldSize(columnTypeArray->data[fieldIndex],rawRowEventDataDetail,leastExecutionUnitEventsForUpdateRowEvent->tableMapEvent->columnMetadataArray[fieldIndex]);
	g_warning("AI fieldIndex=%d,fieldSizePlusLengthBytes=%d",fieldIndex,fieldSizePlusLengthBytes);
        singleRowLevelAfterImageEnd+=fieldSizePlusLengthBytes;
        rawRowEventDataDetail+=fieldSizePlusLengthBytes;
      }
      singleRowLevelAfterImageEnd--;

      exchangeBeforeRowImageWithAfterRowImage(singleRowLevelBeforeImageStart,singleRowLevelBeforeImageEnd,singleRowLevelAfterImageStart,singleRowLevelAfterImageEnd);
      int i;
      i=0;

    }
    rowEventList=rowEventList->next;
  }
}

int reverseLeastExecutionUnitEventsForWriteOrDeleteRowEvent(LeastExecutionUnitEvents *leastExecutionUnitEventsForWriteOrDeleteRowEvent,guint8 targetEventType){

  if( (WRITE_ROWS_EVENT != targetEventType) && ( DELETE_ROWS_EVENT != targetEventType ) ){
    g_warning("Only for delete and write event, but this event type is  %s\n", Binlog_event_type_name[targetEventType]);
    return 0;
  }

  GList * rowEventList = leastExecutionUnitEventsForWriteOrDeleteRowEvent->rowEventList;

  RowEvent * rowEvent;
  while( NULL != rowEventList ){
    rowEvent = rowEventList->data;
    rowEvent->eventHeader->eventType=targetEventType;
    updateRawEventHeaderByModifyConstructMember(rowEvent->eventHeader);
    rowEventList=rowEventList->next;
  }
  return 0;

}


int reverseLeastExecutionUnitEvents(LeastExecutionUnitEvents *leastExecutionUnitEvents){

  guint8 originalRowEventType;
  originalRowEventType=leastExecutionUnitEvents->originalRowEventType;

  switch (originalRowEventType) {
    case UPDATE_ROWS_EVENT:{
        reverseLeastExecutionUnitEventsForUpdateRowEvent(leastExecutionUnitEvents);
        break;
    }
    case WRITE_ROWS_EVENT:{
        reverseLeastExecutionUnitEventsForWriteOrDeleteRowEvent(leastExecutionUnitEvents, DELETE_ROWS_EVENT);
        break;
    }
    case DELETE_ROWS_EVENT:{
        reverseLeastExecutionUnitEventsForWriteOrDeleteRowEvent(leastExecutionUnitEvents,WRITE_ROWS_EVENT);
        break;
    }
  }
  return 0;
}



int appendFormatDescriptionEventToChannel(GIOChannel *ioChannel){
  if(NULL == formatDescriptionEventForGlobalUse){
    g_warning("FormatDescriptionEvent not set, please check");
    return 1;
  }
  GIOStatus ioStatus;
  guint64 bytes_written;
  ioStatus = g_io_channel_write_chars(ioChannel,formatDescriptionEventForGlobalUse->eventHeader->rawEventHeader,EVENT_HEADER_LENGTH,&bytes_written,NULL);
  if  ( G_IO_STATUS_NORMAL != ioStatus){
    g_warning("Failed to write the FormatDescriptionEvent header");
    return 1;
  }

  ioStatus = g_io_channel_write_chars(ioChannel,formatDescriptionEventForGlobalUse->rawFormatDescriptionEventDataDetail,getRawEventDataLengthWithChecksum(formatDescriptionEventForGlobalUse->eventHeader),&bytes_written,NULL);
  if  ( G_IO_STATUS_NORMAL != ioStatus){
    g_warning("Failed to write the FormatDescriptionEvent data");
    return 1;
  }
  return 0;

}

GList* constructBinlogFromEventListWithSizeLimit(GList* allEventsList, gchar* binlogFileName , guint64 maxSplitSize){
  GIOChannel*  binlogOutChannel;
  binlogOutChannel=getIoChannelForWrite(binlogFileName);
  GIOStatus ioStatus;
  guint64 bytes_written;
  ioStatus = g_io_channel_write_chars(binlogOutChannel,MAGIC_HEADER_CONTENT,sizeof(MAGIC_HEADER_CONTENT),&bytes_written,NULL);
  if  ( G_IO_STATUS_NORMAL != ioStatus){
    g_warning("Failed to write the magic word");
    return 1;
  }

  appendFormatDescriptionEventToChannel(binlogOutChannel);
  guint64 currentPos;
  currentPos = sizeof(MAGIC_HEADER_CONTENT) + formatDescriptionEventForGlobalUse->eventHeader->eventLength;

  EventWrapper* eventWrapper=NULL;
  EventHeader* eventHeader=NULL;

  while ( allEventsList ) {
    eventWrapper=allEventsList->data;
    eventHeader=getEventHeaderFromWrapper(eventWrapper);
    currentPos=modifyAndReturnNextEventPos(eventHeader,currentPos);
    ioStatus = g_io_channel_write_chars(binlogOutChannel,eventHeader->rawEventHeader,EVENT_HEADER_LENGTH,&bytes_written,NULL);
    if  ( G_IO_STATUS_NORMAL != ioStatus){
      g_warning("Failed to write the header");
      return NULL;
    }
    ioStatus=g_io_channel_write_chars(binlogOutChannel,getRawEventDataFromWrapper(eventWrapper),getRawEventDataLengthWithChecksum(eventHeader),&bytes_written,NULL);
    if  ( G_IO_STATUS_NORMAL != ioStatus){
      g_warning("Failed to write the  data");
      return NULL;
    }
    if((maxSplitSize>0)&&(currentPos >=maxSplitSize)){
      //we cannot split TableMapEvent with RowEvent
      if( (TRUE == isRowEvent(eventWrapper->eventType))) {
        allEventsList=allEventsList->next;
        break;
      }
    }
    allEventsList=allEventsList->next;
  }
  ioStatus = g_io_channel_flush(binlogOutChannel,NULL);
  if  ( G_IO_STATUS_NORMAL != ioStatus){
    g_warning("Failed to flush, still some events in memory");
    return NULL;
  }
  return allEventsList;

}


GList* splitBigRowEventsToTableMapWithRowEventForEventList(GList* allEventsList){
  GList* splitedEventList=NULL;
  EventWrapper* prevTableMapEventWrapper=NULL;
  guint8 prevEventType=0;
   while(NULL != allEventsList){
     g_warning("in splitBigRowEventsToTableMapWithRowEventForEventList: %s",Binlog_event_type_name[((EventWrapper*)(allEventsList->data))->eventType]);
     if( NULL != allEventsList->prev ){
       prevEventType=((EventWrapper*)(allEventsList->prev->data))->eventType;
     }

     if(isRowEvent(((EventWrapper*)(allEventsList->data))->eventType)) {
       markLastRowEventInStatement(((EventWrapper*)(allEventsList->data)));
     }

     if( (isRowEvent(prevEventType))&& ( isRowEvent(((EventWrapper*)(allEventsList->data))->eventType)) &&(NULL != prevTableMapEventWrapper)){
       splitedEventList=g_list_prepend(splitedEventList,prevTableMapEventWrapper);
       g_warning("TABLE_MAP_EVENT added");
     }
     splitedEventList=g_list_prepend(splitedEventList,(EventWrapper*)(allEventsList->data));

     if( TABLE_MAP_EVENT == ((EventWrapper*)(allEventsList->data))->eventType ){
       prevTableMapEventWrapper=(EventWrapper*)(allEventsList->data);
     }
     allEventsList=allEventsList->next;
   }
   //g_list_free(allEventsList);
   splitedEventList=g_list_reverse(splitedEventList);
   return splitedEventList;

}

int constructBinlogFromEventList(GList* allEventsList){
  guint64 postfixIndex;
  postfixIndex=1;
  gchar* completeFileName;
  allEventsList=splitBigRowEventsToTableMapWithRowEventForEventList(allEventsList);
  while(NULL != allEventsList){
    completeFileName=constructFileNameWithPostfixIndex(optOutBinlogFileNameBase,postfixIndex);
    allEventsList=constructBinlogFromEventListWithSizeLimit(allEventsList,completeFileName,optMaxSplitSize);
    postfixIndex++;
  }

  return 0;
}


int constructBinlogFromLeastExecutionUintList( GList* allLeastExecutionUnitList ){

  LeastExecutionUnitEvents *leastExecutionUnitEvents;
  gchar binlogFlashbackOutFileNamePostfix[]="flashback";
  gchar* binlogFlashbackOutFileName;
  //binlogFlashbackOutFileName=g_new0(gchar, strlen(optOutBinlogFileNameBase)+strlen(binlogFlashbackOutFileNamePostfix)+1+1);
  binlogFlashbackOutFileName = g_strdup_printf("%s.%s",optOutBinlogFileNameBase,binlogFlashbackOutFileNamePostfix);

  GIOChannel*  binlogFlashbackOutChannel;
  binlogFlashbackOutChannel=getIoChannelForWrite(binlogFlashbackOutFileName);
  GIOStatus ioStatus;
  guint64 bytes_written;
  ioStatus = g_io_channel_write_chars(binlogFlashbackOutChannel,MAGIC_HEADER_CONTENT,sizeof(MAGIC_HEADER_CONTENT),&bytes_written,NULL);
  if  ( G_IO_STATUS_NORMAL != ioStatus){
    g_warning("Failed to write the magic word");
    return 1;
  }

  appendFormatDescriptionEventToChannel(binlogFlashbackOutChannel);
  guint64 currentPos;
  currentPos = sizeof(MAGIC_HEADER_CONTENT) + formatDescriptionEventForGlobalUse->eventHeader->eventLength;


  while(NULL != allLeastExecutionUnitList){
    leastExecutionUnitEvents=allLeastExecutionUnitList->data;
    TableMapEvent *tableMapEvent ;
    tableMapEvent=leastExecutionUnitEvents->tableMapEvent;
    currentPos=modifyAndReturnNextEventPos(tableMapEvent->eventHeader,currentPos);
    ioStatus = g_io_channel_write_chars(binlogFlashbackOutChannel,tableMapEvent->eventHeader->rawEventHeader,EVENT_HEADER_LENGTH,&bytes_written,NULL);
    if  ( G_IO_STATUS_NORMAL != ioStatus){
      g_warning("Failed to write the TABLE_MAP_EVENT header");
      return 1;
    }
    ioStatus = g_io_channel_write_chars(binlogFlashbackOutChannel,tableMapEvent->rawTableMapEventDataDetail,getRawEventDataLengthWithChecksum(tableMapEvent->eventHeader),&bytes_written,NULL);
    if  ( G_IO_STATUS_NORMAL != ioStatus){
      g_warning("Failed to write the TABLE_MAP_EVENT data");
      return 1;
    }

    GList *rowEventList;
    RowEvent* rowEvent;
    guint8 eventType;

    rowEventList= leastExecutionUnitEvents->rowEventList;
    while( NULL != rowEventList ){
      rowEvent=rowEventList->data;
      eventType=rowEvent->eventHeader->eventType;
      currentPos=modifyAndReturnNextEventPos(rowEvent->eventHeader,currentPos);
      ioStatus = g_io_channel_write_chars(binlogFlashbackOutChannel,rowEvent->eventHeader->rawEventHeader,EVENT_HEADER_LENGTH, &bytes_written, NULL);
      if  ( G_IO_STATUS_NORMAL != ioStatus){
        g_warning("Failed to write the %s data", Binlog_event_type_name[eventType]);
        return 1;
      }

      ioStatus = g_io_channel_write_chars(binlogFlashbackOutChannel,rowEvent->rawRowEventDataDetail,getRawEventDataLengthWithChecksum(rowEvent->eventHeader), &bytes_written, NULL);
      if  ( G_IO_STATUS_NORMAL != ioStatus){
        g_warning("Failed to write the %s data", Binlog_event_type_name[eventType]);
        return 1;
      }
      rowEventList=rowEventList->next;
    }
    if (!xidEventForGlobalUse){
      g_warning("No XID event found, maybe the position range or datetime range you specified is too small! Try with a larger range");
    }
    //Append Xid event
    currentPos=modifyAndReturnNextEventPos(xidEventForGlobalUse->eventHeader,currentPos);
    ioStatus = g_io_channel_write_chars(binlogFlashbackOutChannel,xidEventForGlobalUse->eventHeader->rawEventHeader, EVENT_HEADER_LENGTH,  &bytes_written, NULL);
    ioStatus = g_io_channel_write_chars(binlogFlashbackOutChannel,xidEventForGlobalUse->rawXidEventDataDetail,getRawEventDataLengthWithChecksum(xidEventForGlobalUse->eventHeader), &bytes_written, NULL);
    
    allLeastExecutionUnitList=allLeastExecutionUnitList->next;

  }
  g_io_channel_flush(binlogFlashbackOutChannel,NULL);
  return 0;
}

int flashbackAllEvents(GList* allEventsList){
  GList *flashbackEventList;
  flashbackEventList=NULL;
  GList *tempAllEventList;
  tempAllEventList=allEventsList;
  EventWrapper* tempEventWrapper;
  while( NULL != tempAllEventList ){
    tempEventWrapper=deepCopyEventWrapper(tempAllEventList->data);
    if(NULL != tempEventWrapper){
      flashbackEventList= g_list_prepend(flashbackEventList,tempEventWrapper);
    }
    tempAllEventList=tempAllEventList->next;
  }
  flashbackEventList=g_list_reverse(flashbackEventList);
  GList *allLeastExecutionUnitList;
  allLeastExecutionUnitList=NULL;
  constructLeastExecutionUnitFromAllEventsList(flashbackEventList,&allLeastExecutionUnitList);
  g_debug("have %u events ", g_list_length(flashbackEventList));
  GList* tempAllLeastExecutionUnitList;
  tempAllLeastExecutionUnitList=allLeastExecutionUnitList;
  while( NULL != tempAllLeastExecutionUnitList ){
    reverseLeastExecutionUnitEvents(tempAllLeastExecutionUnitList->data);
    //printLeastExecutionUnitEventsInHex(tempAllLeastExecutionUnitList->data);

    tempAllLeastExecutionUnitList=tempAllLeastExecutionUnitList->next;
  }

  GList* reverseAllLeastExecutionUnitList;
  reverseAllLeastExecutionUnitList=g_list_reverse(allLeastExecutionUnitList);
  constructBinlogFromLeastExecutionUintList(reverseAllLeastExecutionUnitList);

  return 0;

}





int processBinlog(GIOChannel * binlogGlibChannel,guint64 fileIndex, gboolean isLastFile){
	guint64 MagicHeaderLength=4;
	guint64 currentPos;
	currentPos=MagicHeaderLength;
	GIOStatus ioStatus;
	ioStatus=g_io_channel_seek_position(binlogGlibChannel,currentPos,G_SEEK_SET,NULL);
	if(G_IO_STATUS_NORMAL != ioStatus ){
		g_warning("failed to advance the offset: %ld ", MagicHeaderLength);
	}

	gchar* headerBuffer;
	headerBuffer=(gchar*)malloc(MAX_HEADER_LENGTH);
	guint64 realHeaderLength;
  GList *allEventsList = NULL;
  gboolean isShouldDiscardForGtid=FALSE;
  gboolean isFirstXidEventAppeared=FALSE;
	while( G_IO_STATUS_NORMAL == (ioStatus = g_io_channel_read_chars(binlogGlibChannel,headerBuffer,EVENT_HEADER_LENGTH,&realHeaderLength,NULL))){
		EventHeader *eventHeader;
		eventHeader=g_new0(EventHeader,1);
		parseHeader(headerBuffer,eventHeader);
		g_warning("currentPos %llu \n", currentPos);
		g_warning("eventHeader %llu \n", eventHeader->nextEventPos);
		currentPos += EVENT_HEADER_LENGTH;
		if( G_IO_STATUS_NORMAL != (ioStatus=g_io_channel_seek_position(binlogGlibChannel,currentPos,G_SEEK_SET,NULL)) ){
			g_warning("failed to seek the pos %ld \n", currentPos);
		}

		gchar* dataBuffer = g_new0(gchar , (eventHeader->eventLength-EVENT_HEADER_LENGTH));
		guint64 realDataLength;

		if (G_IO_STATUS_NORMAL == (ioStatus = g_io_channel_read_chars(binlogGlibChannel,dataBuffer,(eventHeader->eventLength-EVENT_HEADER_LENGTH),&realDataLength,NULL)) ){
			g_warning("event type: %s ",Binlog_event_type_name[eventHeader->eventType]);
			switch(eventHeader->eventType){
				case TABLE_MAP_EVENT:{
					TableMapEvent *tableMapEvent = g_new0(TableMapEvent,1);
					initTableMapEvent(tableMapEvent,eventHeader,dataBuffer);
					parseTableMapEventData(dataBuffer,tableMapEvent);
          appendToAllEventList(&allEventsList,eventHeader,(gpointer)tableMapEvent);

					break;
				}
				case FORMAT_DESCRIPTION_EVENT:{
					FormatDescriptionEvent * formatDescriptionEvent=g_new0(FormatDescriptionEvent,1);
					initFormatDescriptionEvent(formatDescriptionEvent, eventHeader, dataBuffer);
					setFormatDescriptionEventForGlobalUse(formatDescriptionEvent);
          g_debug("just skip the FORMAT_DESCRIPTION_EVENT \n");

					break;
				}
				case WRITE_ROWS_EVENT:
				case UPDATE_ROWS_EVENT:
				case DELETE_ROWS_EVENT:{
					RowEvent *rowEvent = g_new0(RowEvent,1);
					initRowEvent(rowEvent,eventHeader,dataBuffer);
          appendToAllEventList(&allEventsList,eventHeader,(gpointer)rowEvent);

					break;
				}
        case QUERY_EVENT:{
          QueryEvent *queryEvent = g_new0(QueryEvent,1);
          initQueryEvent(queryEvent,eventHeader,dataBuffer);
          appendToAllEventList(&allEventsList,eventHeader,(gpointer)queryEvent);
          //g_list_append(allEventsList,( gpointer )eventWrapper);

          break;
        }
        case XID_EVENT:{
        if (FALSE == isFirstXidEventAppeared){
          XidEvent *xidEvent= g_new0(XidEvent,1);
          initXidEvent(xidEvent,eventHeader,dataBuffer);
          //appendToAllEventList(&allEventsList,eventHeader,(gpointer)xidEvent);
          setXidEventForGlobalUse(xidEvent);
          isFirstXidEventAppeared = TRUE;
          break;
        }
        break;
        }
        case GTID_LOG_EVENT:{
          GtidEvent *gtidEvent= g_new0(GtidEvent,1);
          initGtidEvent(gtidEvent,eventHeader,dataBuffer);
          parseGtidEvent(dataBuffer,gtidEvent);
          appendToAllEventList(&allEventsList,eventHeader,(gpointer)gtidEvent);
          isShouldDiscardForGtid=isTransactionShouldDiscardForGtid(gtidEvent);
          break;
        }

			}


		}
		currentPos += (eventHeader->eventLength - EVENT_HEADER_LENGTH);

    gboolean isShouldStop=FALSE;
    isShouldStop=getNextPosOrStop(&currentPos,fileIndex,isLastFile);
    if (TRUE == isShouldStop ) {
      g_warning("we have reach the stop pos, stop parsing");
      break;
    }

    guint8 statusForDatatime;
    statusForDatatime=isShouldStopOrDiscardForDateTimeRange(eventHeader->binlogTimestamp);
    if (STOP == statusForDatatime ) {
      g_warning("we have reach the stop pos, stop parsing");
      break;
    }else if((DISCARD == statusForDatatime) && ( FORMAT_DESCRIPTION_EVENT != eventHeader->eventType)) {
      g_warning("we discard the event for it less than the startDatetime");
      GList* head = allEventsList;
      allEventsList=g_list_delete_link(allEventsList,head);
    }

    if((TRUE == isShouldDiscardForGtid) && isConsideredEventType(eventHeader->eventType) && (eventHeader->eventType != XID_EVENT)){
      g_warning("we discard it for gtid setting");
      GList* head = allEventsList;
      allEventsList=g_list_delete_link(allEventsList,head);
    }

		if( G_IO_STATUS_NORMAL != (ioStatus=g_io_channel_seek_position(binlogGlibChannel,currentPos,G_SEEK_SET,NULL)) ){
			g_warning("failed to seek the pos %ld ", currentPos);
		}

		headerBuffer=(gchar*)malloc(MAX_HEADER_LENGTH);

	}



  g_debug("have %u events ", g_list_length(allEventsList));

  allEventsList=g_list_reverse(allEventsList);
  if(optMaxSplitSize>0){
    constructBinlogFromEventList(allEventsList);
    g_warning("just split the binlog ");
    return 0;
  }

  flashbackAllEvents(allEventsList);





}

int64_t S64(const char *s) {
  int64_t i;
  char c ;
  int scanned = sscanf(s, "%" SCNd64 "%c", &i, &c);
  if (scanned == 1) return i;
  if (scanned > 1) {
    // TBD about extra data found
    return i;
    }
  // TBD failed to scan;
  return 0;
}


GArray* parsemultipleGtidSetToGtidSetInfoArray(gchar* multipleGtidSet ){
  if(NULL == multipleGtidSet){
    return NULL;
  }
  GArray* gtidSetArray;
  gtidSetArray=g_array_new(FALSE,TRUE,sizeof(GtidSetInfo));
  gchar** gtidSets;
  gchar** uuidWithSeqNos;
  gtidSets = g_strsplit(multipleGtidSet,",",0);
  guint64 tempSeqNo;

  gchar** seqNos;
  int i=0;
  while( gtidSets[i] ){
    GtidSetInfo* gtidSetInfo = g_new0(GtidSetInfo,1);
    uuidWithSeqNos=g_strsplit(gtidSets[i],":",0);
    gtidSetInfo->uuid=packUuidInto16Bytes(uuidWithSeqNos[0]);
    seqNos = g_strsplit(uuidWithSeqNos[1],"-",0);

    //:12
    if( (2 > g_strv_length(seqNos)) ){
        gtidSetInfo->startSeqNo=S64(seqNos[0]);
        gtidSetInfo->stopSeqNo=S64(seqNos[0]);
    }
    //:12-100
    else{
        gtidSetInfo->startSeqNo=S64(seqNos[0]);
        gtidSetInfo->stopSeqNo=S64(seqNos[1]);
    }

    g_array_append_val(gtidSetArray,*gtidSetInfo);
    i++;
  }

  return gtidSetArray;

}


GHashTable* parseNames(gchar* names){
	if (NULL == names){
		return NULL;
	}
	gchar **nameArray;
	nameArray=g_strsplit(names, ",", 0);
	int i=0;
	GHashTable* namesHash = NULL;
	if (nameArray[0]){
		namesHash = g_hash_table_new(g_str_hash, g_str_equal);
	}
	while( nameArray[i] ){
		g_hash_table_insert(namesHash,g_strndup(nameArray[i], strlen(nameArray[i])),g_new0(gchar, 1));	
		i++;
	}
	return namesHash;
} 

int parseOption(int argc, char **argv){

  GError *error = NULL;
  GOptionContext *context;
  GOptionEntry entries [] =
    { { "databaseNames", 0, 0, G_OPTION_ARG_STRING, &optDatabaseNames, "databaseName to apply. if multiple, seperate by comma(,)", NULL },
      { "tableNames", 0, 0, G_OPTION_ARG_STRING, &optTableNames, "tableName to apply. if multiple, seperate by comma(,)", NULL },
      { "start-position", 0, 0, G_OPTION_ARG_INT, &optStartPos, "start position", NULL },
      { "stop-position", 0, 0, G_OPTION_ARG_INT, &optStopPos, "stop position", NULL },
      { "start-datetime", 0, 0, G_OPTION_ARG_STRING, &optStartDatetimeStr, "start time (format %Y-%m-%d %H:%M:%S)", NULL },
      { "stop-datetime", 0, 0, G_OPTION_ARG_STRING, &optStopDatetimeStr, "stop time (format %Y-%m-%d %H:%M:%S)", NULL },
      { "sqlTypes", 0, 0, G_OPTION_ARG_STRING, &optSqlTypes, "sql type to filter . support INSERT, UPDATE ,DELETE. if multiple, seperate by comma(,)", NULL },
      { "maxSplitSize", 0, 0, G_OPTION_ARG_INT, &optMaxSplitSize, "max file size after split, the uint is M", NULL },
      { "binlogFileNames", 0, 0, G_OPTION_ARG_STRING, &optBinlogFiles, "binlog files to process. if multiple, seperate by comma(,)  ", NULL },
      { "outBinlogFileNameBase", 0, 0, G_OPTION_ARG_STRING, &optOutBinlogFileNameBase, "output binlog file name base", NULL },
      { "logLevel", 0, 0, G_OPTION_ARG_STRING, &optLogLevel, "log level, available option is debug,warning,error", NULL },
      { "include-gtids",0,0,G_OPTION_ARG_STRING, &optIncludeGtids, "gtids to process", NULL },
      { "exclude-gtids",0,0,G_OPTION_ARG_STRING, &optExcludeGtids, "gtids to skip", NULL },
      { NULL }
    };

   context = g_option_context_new (NULL);
   g_option_context_add_main_entries (context, entries, NULL);
   if (!g_option_context_parse (context, &argc, &argv, &error))
   {
     g_warning("option parsing failed: %s\n", error->message);
     exit (1);
   }

   if( NULL == optOutBinlogFileNameBase ){
     gchar defaultBaseName[]="binlog_output_base";
     optOutBinlogFileNameBase=g_new0(gchar, strlen(defaultBaseName)+1);
     memcpy(optOutBinlogFileNameBase,defaultBaseName,strlen(defaultBaseName)+1);
   }
   checkPotentialConflictOutputFile(optOutBinlogFileNameBase);
   //convert MB to B
   optMaxSplitSize=optMaxSplitSize*1024*1024;

   if( NULL == optLogLevel ){
     gchar defaultLogLevel[]="error";
     optLogLevel=g_new0(gchar , strlen(defaultLogLevel)+1);
     memcpy(optLogLevel,defaultLogLevel,strlen(defaultLogLevel)+1);
   }

   struct tm tm;
   //time_t startTimeStamp;
   //time_t stopTimeStamp;

   if(NULL != optStartDatetimeStr){
     if ( strptime(optStartDatetimeStr, "%Y-%m-%d %H:%M:%S", &tm) != NULL ){
       globalStartTimestamp = mktime(&tm);
     }else {
       g_error("failed to parsing startDatetimeStr");
     }
   }else {
     globalStartTimestamp=0;
   }

   if(NULL != optStopDatetimeStr){
     if ( strptime(optStopDatetimeStr, "%Y-%m-%d %H:%M:%S", &tm) != NULL ){
       globalStopTimestamp = mktime(&tm);
     }else {
       g_error("failed to parsing stopDatetimeStr");
     }
   }else {
     globalStopTimestamp=G_MAXUINT32;
   }

   globalIncludeGtidsArray=parsemultipleGtidSetToGtidSetInfoArray(optIncludeGtids);
   globalExcludeGtidsArray=parsemultipleGtidSetToGtidSetInfoArray(optExcludeGtids);
	 tableNamesHash = parseNames(optTableNames); 
	 databaseNamesHash = parseNames(optDatabaseNames); 

   return 0;

}

static void _dummy(const gchar *log_domain,
                     GLogLevelFlags log_level,
                     const gchar *message,
                     gpointer user_data )

{
  /* Dummy does nothing */
  return ;
}

int setLogHandler(){

  g_log_set_handler(G_LOG_DOMAIN, G_LOG_LEVEL_MASK, _dummy, NULL);
  if( 0 == g_ascii_strcasecmp(optLogLevel,"debug") ){
     g_log_set_handler(G_LOG_DOMAIN, G_LOG_LEVEL_DEBUG,  g_log_default_handler, NULL);
  }else if(  0 == g_ascii_strcasecmp(optLogLevel,"warning")) {
     g_log_set_handler(G_LOG_DOMAIN, G_LOG_LEVEL_WARNING, g_log_default_handler, NULL);
  }else {
    g_log_set_handler(G_LOG_DOMAIN, G_LOG_LEVEL_ERROR, g_log_default_handler, NULL);
  }

  return 0;

}


int main(int argc, char **argv){

  parseOption(argc,argv);
  setLogHandler();
  gchar **binlogFileNameArray;
  if(NULL == optBinlogFiles){
    g_warning("please specify the binlog file");
    exit(1);
  }
  binlogFileNameArray=g_strsplit(optBinlogFiles,",",0);
  guint64 binlogFileNameArraySize=0;


  while (binlogFileNameArray[binlogFileNameArraySize]) {
    binlogFileNameArraySize++;
  }

  guint64 i=0;

  while( binlogFileNameArray[i] ){
	if( access(binlogFileNameArray[i] , F_OK ) == -1 ) {
 		g_error("binlog:%s does not exist\n",binlogFileNameArray[i]);
		return 1;
	}



  	GIOChannel * binlogGlibChannel;
  	binlogGlibChannel = g_io_channel_new_file(binlogFileNameArray[i],"r",NULL);
  	if (NULL == binlogGlibChannel){
  		g_warning("failed to open %s \n", binlogFileNameArray[i]);
  	}
  	GIOStatus encodingSetStatus;
  	encodingSetStatus = g_io_channel_set_encoding(binlogGlibChannel,NULL,NULL);
  	if (G_IO_STATUS_NORMAL != encodingSetStatus ){
  		g_warning("failed to set to binary mode \n");
  	}
    rotateOutputBinlogFileNames(optOutBinlogFileNameBase,0);
	  processBinlog(binlogGlibChannel,i, ( i == (binlogFileNameArraySize-1) ) );
     i++;
   }

	return 0;


}
