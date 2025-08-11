#ifndef _DCDN_UTIL_HTTP_CLIENT_H_
#define _DCDN_UTIL_HTTP_CLIENT_H_

#include <curl/curl.h>

#include <string>
#include <unordered_map>

#include "common/Common.h"

NS_BEGIN(dcdn)
NS_BEGIN(util)

class HttpClient;

class HttpHeaders
{
public:
    class CurlHeaders
    {
    public:
        CurlHeaders(): mHeaders(nullptr) {}
        CurlHeaders(const std::unordered_multimap<std::string, std::string>& headers)
        {
            std::string header;
            for (auto& it : headers) {
                header = it.first;
                header += ": ";
                header += it.second;
                mHeaders = curl_slist_append(mHeaders, header.c_str());
            }
        }
        CurlHeaders(CurlHeaders&& oth): mHeaders(oth.mHeaders)
        {
            oth.mHeaders = nullptr;
        }
        CurlHeaders& operator=(CurlHeaders&& oth)
        {
            if (mHeaders) {
                curl_slist_free_all(mHeaders);
            }
            mHeaders = oth.mHeaders;
            oth.mHeaders = nullptr;
            return *this;
        }
        CurlHeaders(const CurlHeaders&) = delete;
        CurlHeaders& operator=(const CurlHeaders&) = delete;
        ~CurlHeaders()
        {
            if (mHeaders) {
                curl_slist_free_all(mHeaders);
                mHeaders = nullptr;
            }
        }
        operator curl_slist*() const
        {
            return mHeaders;
        }
        operator bool() const
        {
            return mHeaders != nullptr;
        }

    private:
        curl_slist* mHeaders = nullptr;
    };

public:
    HttpHeaders() {}
    ~HttpHeaders()
    {
        Clear();
    }
    void Clear()
    {
        mHeaders.clear();
    }
    bool Empty() const
    {
        return mHeaders.empty();
    }
    bool Has(const char* key) const
    {
        auto it = mHeaders.find(key);
        return it != mHeaders.end();
    }
    void Set(const char* key, const char* val, bool unique = true)
    {
        if (unique) {
            mHeaders.erase(key);
        }
        mHeaders.insert({key, val});
    }
    void Remove(const char* key)
    {
        auto it = mHeaders.find(key);
        if (it != mHeaders.end()) {
            mHeaders.erase(it);
        }
    }
    CurlHeaders Curl() const
    {
        return CurlHeaders(mHeaders);
    }

private:
    std::unordered_multimap<std::string, std::string> mHeaders;
};

class HttpRequest
{
public:
    enum MethodType
    {
        Get = 1,
        Post,
    };

public:
    HttpRequest(): mMethod(Get) {}
    HttpRequest(const std::string& url): mMethod(Get), mUrl(url) {}
    HttpRequest(const std::string& url, const std::string& body, const char* contentType = nullptr)
        : mMethod(Post), mUrl(url)
    {
        SetBody(body, contentType);
    }
    HttpRequest(HttpRequest&& oth)
        : mMethod(oth.mMethod),
          mUrl(std::move(oth.mUrl)),
          mHeaders(std::move(oth.mHeaders)),
          mBody(std::move(oth.mBody))
    {
    }
    ~HttpRequest() {}
    HttpRequest& operator=(HttpRequest&& oth)
    {
        mMethod = oth.mMethod;
        mUrl = std::move(oth.mUrl);
        mHeaders = std::move(oth.mHeaders);
        mBody = std::move(oth.mBody);
        return *this;
    }
    MethodType Method() const
    {
        return mMethod;
    }
    void SetMethod(MethodType tp)
    {
        mMethod = tp;
    }
    void SetUrl(const char* url)
    {
        mUrl = url;
    }
    const std::string& Url() const
    {
        return mUrl;
    }
    bool HasHeader() const
    {
        return !mHeaders.Empty();
    }
    void SetHeader(const char* key, const char* val)
    {
        mHeaders.Set(key, val);
    }
    void ClearHeaders()
    {
        mHeaders.Clear();
    }
    const HttpHeaders& Headers() const
    {
        return mHeaders;
    }
    void SetUserAgent(const char* val)
    {
        mHeaders.Set("User-Agent", val);
    }
    void SetContentType(const char* val)
    {
        mHeaders.Set("Content-Type", val);
    }
    void SetBody(const std::string& body, const char* contentType = nullptr)
    {
        mBody = body;
        if (contentType) {
            SetHeader("Content-Type", contentType);
        } else if (!mHeaders.Has("Content-Type")) {
            SetHeader("Content-Type", "x-www-form-urlencoded");
        }
        char buf[128];
        snprintf(buf, sizeof(buf), "%llu", (unsigned long long)body.size());
        SetHeader("Content-Length", buf);
    }
    const std::string& Body() const
    {
        return mBody;
    }

private:
    MethodType mMethod;
    std::string mUrl;
    HttpHeaders mHeaders;
    std::string mBody;
};

class HttpResponse
{
public:
    HttpResponse() {}
    HttpResponse(HttpResponse&& oth)
        : mStatus(oth.mStatus), mHeaders(std::move(oth.mHeaders)), mBody(std::move(oth.mBody))
    {
    }
    HttpResponse& operator=(HttpResponse&& oth)
    {
        mStatus = oth.mStatus;
        mHeaders = std::move(oth.mHeaders);
        mBody = std::move(oth.mBody);
        return *this;
    }
    void SetStatus(long status)
    {
        mStatus = status;
    }
    long Status() const
    {
        return mStatus;
    }
    HttpHeaders& Headers()
    {
        return mHeaders;
    }
    const HttpHeaders& Headers() const
    {
        return mHeaders;
    }
    std::string& Body()
    {
        return mBody;
    }
    const std::string& Body() const
    {
        return mBody;
    }

private:
    long mStatus;
    HttpHeaders mHeaders;
    std::string mBody;
};

class HttpClientOption
{
public:
    HttpClientOption() {}
    HttpClientOption(HttpClientOption&& oth)
    {
        operator=(std::move(oth));
    }
    HttpClientOption& operator=(HttpClientOption&& oth)
    {
        mUserAgent = std::move(oth.mUserAgent);
        mFollowLocation = oth.mFollowLocation;
        mConnectTimeout = oth.mConnectTimeout;
        mRequestTimeout = oth.mRequestTimeout;
        mVerifySsl = oth.mVerifySsl;

        oth.mFollowLocation = 0;
        oth.mConnectTimeout = 0;
        oth.mRequestTimeout = 0;
        oth.mVerifySsl = true;

        return *this;
    }
    void SetUserAgent(const char* val)
    {
        mUserAgent = val;
    }
    const std::string& UserAgent() const
    {
        return mUserAgent;
    }
    void SetFollowLocation(long val)
    {
        mFollowLocation = val;
    }
    long followLocation() const
    {
        return mFollowLocation;
    }
    void SetConnectionTimeout(long ms)
    {
        mConnectTimeout = ms;
    }
    long ConnectTimeout() const
    {
        return mConnectTimeout;
    }
    void SetRequestTimeout(long ms)
    {
        mRequestTimeout = ms;
    }
    long RequestTimeout() const
    {
        return mRequestTimeout;
    }
    void SetVerifySsl(bool val)
    {
        mVerifySsl = val;
    }
    bool VerifySsl() const
    {
        return mVerifySsl;
    }

protected:
    void fill(CURL* c)
    {
        if (!mUserAgent.empty()) {
            curl_easy_setopt(c, CURLOPT_USERAGENT, mUserAgent.c_str());
        }
        if (mFollowLocation > 0) {
            curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, mFollowLocation);
        }
        if (mConnectTimeout > 0) {
            curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT_MS, mConnectTimeout);
        }
        if (mRequestTimeout > 0) {
            curl_easy_setopt(c, CURLOPT_TIMEOUT_MS, mRequestTimeout);
        }
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, mVerifySsl ? 1L : 0L);
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, mVerifySsl ? 2L : 0L);
    }

protected:
    std::string mUserAgent;
    long mFollowLocation = 0;
    long mConnectTimeout = 0; // milli seconds
    long mRequestTimeout = 0; // milli seconds
    bool mVerifySsl = true;
};

class HttpClient: public HttpClientOption
{
public:
    HttpClient() {}
    HttpClient(HttpClient&& oth)
    {
        operator=(std::move(oth));
    }
    HttpClient& operator=(HttpClient&& oth)
    {
        mCurl = oth.mCurl;
        oth.mCurl = nullptr;
        HttpClientOption::operator=(std::move(oth));
        return *this;
    }

    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;
    ~HttpClient()
    {
        if (mCurl) {
            curl_easy_cleanup(mCurl);
        }
    }
    long Do(const HttpRequest& req, HttpResponse* resp)
    {
        auto c = getCurl();
        if (!c) {
            return -1;
        }
        curl_easy_setopt(c, CURLOPT_URL, req.Url().c_str());
        fill(c);
        HttpHeaders::CurlHeaders headers(std::move(req.Headers().Curl()));
        if (headers) {
            curl_easy_setopt(c, CURLOPT_HTTPHEADER, (curl_slist*)headers);
        }
        if (req.Method() == HttpRequest::Post) {
            curl_easy_setopt(c, CURLOPT_POST, 1L);
            curl_easy_setopt(c, CURLOPT_POSTFIELDS, req.Body().data());
            curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, static_cast<long>(req.Body().size()));
        }
        if (resp) {
            curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, writeStringCallback);
            curl_easy_setopt(c, CURLOPT_WRITEDATA, &resp->Body());
            if (!resp->Headers().Empty()) {
                curl_easy_setopt(c, CURLOPT_HEADERFUNCTION, writeHeaderCallback);
                curl_easy_setopt(c, CURLOPT_HEADERDATA, &resp->Headers());
            }
        }
        auto res = curl_easy_perform(c);
        if (res != CURLE_OK) {
            return -1;
        }
        long code = 0;
        curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &code);
        if (resp) {
            resp->SetStatus(code);
        }
        return code;
    }
    long Get(const char* url, std::string& response)
    {
        HttpRequest req(url);
        HttpResponse res;
        res.Headers().Set("*", "");
        auto code = Do(req, &res);
        response.swap(res.Body());
        return code;
    }
    long Post(const char* url, const std::string& body, std::string& response, const char* contentType = nullptr)
    {
        HttpRequest req(url, body, contentType);
        HttpResponse res;
        auto code = Do(req, &res);
        response.swap(res.Body());
        return code;
    }

private:
    CURL* getCurl()
    {
        if (mCurl) {
            curl_easy_reset(mCurl);
        } else {
            mCurl = curl_easy_init();
        }
        return mCurl;
    }
    static size_t writeStringCallback(void* body, size_t size, size_t nmemb, void* data)
    {
        auto sz = size * nmemb;
        std::string* s = (std::string*)data;
        s->append((const char*)body, sz);
        return sz;
    }
    static size_t writeHeaderCallback(void* body, size_t size, size_t nmemb, void* data)
    {
        size_t n = size * nmemb;
        HttpHeaders* headers = (HttpHeaders*)data;
        size_t i = 0;
        char* p = (char*)body;
        while (i < n && p[i] != ':') {
            ++i;
        }
        if (i < n) {
            p[i] = 0;
            const char* key = p;
            std::string val;
            if (++i < n) {
                if (p[i] == ' ') {
                    ++i;
                }
                if (i < n) {
                    while (i < n && (p[n - 1] == '\r' || p[n - 1] == '\n')) {
                        --n;
                    }
                    val.assign(p + i, n - i);
                }
            }
            headers->Set(key, val.c_str(), false);
        }
        return size * nmemb;
    }

private:
    CURL* mCurl = nullptr;
};

NS_END
NS_END

#endif
