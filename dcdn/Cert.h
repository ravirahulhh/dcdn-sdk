#ifndef _DCDN_SDK_CERT_H_
#define _DCDN_SDK_CERT_H_

#include <string>

struct CertificatePair
{
    std::string certPem;
    std::string keyPem;
};

CertificatePair generate_ecdsa_certificate();

#endif
