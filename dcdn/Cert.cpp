#include "Cert.h"

#include <openssl/bio.h> // BIO 内存流
#include <openssl/ec.h> // EC_KEY、EC 参数、椭圆曲线生成
#include <openssl/err.h> // 错误处理（可选）
#include <openssl/evp.h> // EVP_PKEY，高层加密密钥封装
#include <openssl/pem.h> // PEM 编码读写
#include <openssl/x509.h> // X509 证书相关操作

#include <stdexcept>

// RAII helpers
// template<typename T>
// using openssl_ptr = std::unique_ptr<T, decltype(&::ASN1_ITEM_free)>;

CertificatePair generate_ecdsa_certificate()
{
    CertificatePair result;

    // 1. Generate EC key (P-256)
    EC_KEY* ec_key = EC_KEY_new_by_curve_name(NID_X9_62_prime256v1);
    if (!ec_key || !EC_KEY_generate_key(ec_key)) {
        throw std::runtime_error("Failed to generate EC key");
    }

    // Convert to EVP_PKEY
    EVP_PKEY* pkey = EVP_PKEY_new();
    if (!EVP_PKEY_assign_EC_KEY(pkey, ec_key)) {
        EC_KEY_free(ec_key);
        throw std::runtime_error("Failed to assign EC key to EVP_PKEY");
    }

    // 2. Create X.509 certificate
    X509* cert = X509_new();
    ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
    X509_gmtime_adj(X509_get_notBefore(cert), 0);
    X509_gmtime_adj(X509_get_notAfter(cert), 31536000L); // 1 year

    X509_set_pubkey(cert, pkey);

    X509_NAME* name = X509_get_subject_name(cert);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, (const unsigned char*)"webrtc.generated", -1, -1, 0);
    X509_set_issuer_name(cert, name);

    if (!X509_sign(cert, pkey, EVP_sha256())) {
        throw std::runtime_error("Failed to sign certificate");
    }

    // 3. Write to memory (PEM)
    BIO* cert_bio = BIO_new(BIO_s_mem());
    PEM_write_bio_X509(cert_bio, cert);
    char* cert_data;
    long cert_len = BIO_get_mem_data(cert_bio, &cert_data);
    result.certPem.assign(cert_data, cert_len);
    BIO_free(cert_bio);

    BIO* key_bio = BIO_new(BIO_s_mem());
    PEM_write_bio_PrivateKey(key_bio, pkey, nullptr, nullptr, 0, nullptr, nullptr);
    char* key_data;
    long key_len = BIO_get_mem_data(key_bio, &key_data);
    result.keyPem.assign(key_data, key_len);
    BIO_free(key_bio);

    // Cleanup
    X509_free(cert);
    EVP_PKEY_free(pkey); // also frees ec_key

    return result;
}
