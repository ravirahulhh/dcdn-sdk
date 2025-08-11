#ifndef _DCDN_COMMON_LOGGER_H_
#define _DCDN_COMMON_LOGGER_H_

#include <plog/Log.h>

#ifndef DCDN_LOGGER_ID
#define DCDN_LOGGER_ID 99
#endif

#define logVerb PLOG_VERBOSE_(DCDN_LOGGER_ID)
#define logDebug PLOG_DEBUG_(DCDN_LOGGER_ID)
#define logInfo PLOG_INFO_(DCDN_LOGGER_ID)
#define logWarn PLOG_WARNING_(DCDN_LOGGER_ID)
#define logError PLOG_ERROR_(DCDN_LOGGER_ID)

#endif
