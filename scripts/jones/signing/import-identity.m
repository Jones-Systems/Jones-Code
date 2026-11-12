#import <Foundation/Foundation.h>
#import <Security/Security.h>

int main(int argc, const char *argv[]) {
  @autoreleasepool {
    if (argc != 2) return 2;
    SecKeychainSetUserInteractionAllowed(false);
    NSData *input = [[NSFileHandle fileHandleWithStandardInput] readDataToEndOfFile];
    NSDictionary *request = [NSJSONSerialization JSONObjectWithData:input options:0 error:nil];
    if (![request isKindOfClass:[NSDictionary class]] ||
        ![request[@"certificate"] isKindOfClass:[NSString class]] ||
        ![request[@"password"] isKindOfClass:[NSString class]]) return 3;
    NSData *p12 = [[NSData alloc] initWithBase64EncodedString:request[@"certificate"] options:0];
    if (p12 == nil) return 4;
    SecKeychainRef keychain = NULL;
    OSStatus status = SecKeychainOpen(argv[1], &keychain);
    if (status != errSecSuccess) return 5;
    SecExternalFormat format = kSecFormatPKCS12;
    SecExternalItemType type = kSecItemTypeAggregate;
    SecItemImportExportKeyParameters parameters = {0};
    parameters.version = SEC_KEY_IMPORT_EXPORT_PARAMS_VERSION;
    // The real P12 password exists only in this process, never in argv or a file.
    parameters.passphrase = (__bridge CFTypeRef)request[@"password"];
    status = SecItemImport((__bridge CFDataRef)p12, NULL, &format, &type, 0,
                          &parameters, keychain, NULL);
    CFRelease(keychain);
    return status == errSecSuccess ? 0 : 6;
  }
}
