#ifndef _DCDN_SDK_SQLITE_ORM_HELPER_H_
#define _DCDN_SDK_SQLITE_ORM_HELPER_H_

#include "common/Common.h"

NS_BEGIN(dcdn)

template<auto Func, typename Storage = decltype(Func(""))>
class StorageRefImpl
{
public:
    typedef StorageRefImpl<Func, Storage> Base;

    Storage stor;

    StorageRefImpl(const std::string& filename): stor(Func(filename)) {}
};

NS_END

#endif
