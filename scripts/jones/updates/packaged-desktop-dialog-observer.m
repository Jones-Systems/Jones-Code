#import <Foundation/Foundation.h>
#import <ApplicationServices/ApplicationServices.h>
#include <libproc.h>
#include <sys/stat.h>
#include <limits.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

static pid_t targetPid;
static NSString *targetPath, *code, *transactionId;
static uint64_t startSec, startUsec;
static AXUIElementRef application;
static BOOL finished = NO;
static BOOL mainWindowMode = NO;
static int resultCode = 1;
static CFAbsoluteTime deadline;

static void emit(NSDictionary *record) {
    NSData *data = [NSJSONSerialization dataWithJSONObject:record options:0 error:nil];
    if (!data || data.length > 65536) { resultCode = 1; finished = YES; return; }
    fwrite(data.bytes, 1, data.length, stdout);
    fputc('\n', stdout);
    fflush(stdout);
}

static BOOL identity(BOOL capture) {
    char path[PROC_PIDPATHINFO_MAXSIZE];
    struct proc_bsdinfo info;
    if (proc_pidpath(targetPid, path, sizeof(path)) <= 0 ||
        proc_pidinfo(targetPid, PROC_PIDTBSDINFO, 0, &info, sizeof(info)) != sizeof(info)) return NO;
    if (![targetPath isEqualToString:[NSString stringWithUTF8String:path]]) return NO;
    if (capture) {
        startSec = info.pbi_start_tvsec;
        startUsec = info.pbi_start_tvusec;
    }
    return startSec > 0 && info.pbi_start_tvsec == startSec && info.pbi_start_tvusec == startUsec;
}

static id attribute(AXUIElementRef element, CFStringRef name) {
    if (CFAbsoluteTimeGetCurrent() >= deadline) return nil;
    AXUIElementSetMessagingTimeout(element, 0.05f);
    CFTypeRef value = NULL;
    if (AXUIElementCopyAttributeValue(element, name, &value) != kAXErrorSuccess) return nil;
    return CFBridgingRelease(value);
}

static NSString *textAttribute(AXUIElementRef element, CFStringRef name) {
    id value = attribute(element, name);
    return [value isKindOfClass:[NSString class]] && [value length] <= 4096 ? value : @"";
}

static BOOL collect(AXUIElementRef element, NSUInteger depth, NSUInteger *count,
                    NSMutableArray<NSString *> *texts, NSMutableArray *buttons) {
    if (++*count > 128 || depth > 8 || CFAbsoluteTimeGetCurrent() >= deadline) return NO;
    for (id key in @[(__bridge id)kAXTitleAttribute, (__bridge id)kAXDescriptionAttribute, (__bridge id)kAXValueAttribute]) {
        NSString *text = textAttribute(element, (__bridge CFStringRef)key);
        if (text.length) [texts addObject:text];
    }
    NSString *role = textAttribute(element, kAXRoleAttribute);
    if ([role isEqualToString:@"AXButton"] && [textAttribute(element, kAXTitleAttribute) isEqualToString:@"OK"])
        [buttons addObject:(__bridge id)element];
    id children = attribute(element, kAXChildrenAttribute);
    if ([children isKindOfClass:[NSArray class]]) {
        for (id child in children) {
            if (CFGetTypeID((__bridge CFTypeRef)child) != AXUIElementGetTypeID() ||
                !collect((__bridge AXUIElementRef)child, depth + 1, count, texts, buttons)) return NO;
        }
    }
    return YES;
}


static void inspectMainWindow(void) {
    if (finished || !identity(NO)) return;
    id windows = attribute(application, kAXWindowsAttribute);
    if (![windows isKindOfClass:[NSArray class]] || [windows count] > 16) return;
    for (id window in windows) {
        AXUIElementRef element = (__bridge AXUIElementRef)window;
        if (CFGetTypeID(element) != AXUIElementGetTypeID()) continue;
        pid_t windowPid = 0;
        if (AXUIElementGetPid(element, &windowPid) != kAXErrorSuccess || windowPid != targetPid) continue;
        NSString *role = textAttribute(element, kAXRoleAttribute);
        NSString *subrole = textAttribute(element, kAXSubroleAttribute);
        id main = attribute(element, kAXMainAttribute);
        id minimized = attribute(element, kAXMinimizedAttribute);
        id value = attribute(element, kAXSizeAttribute);
        CGSize size = CGSizeZero;
        if (![role isEqualToString:@"AXWindow"] || ![subrole isEqualToString:@"AXStandardWindow"] ||
            ![main isKindOfClass:[NSNumber class]] || ![main boolValue] ||
            ![minimized isKindOfClass:[NSNumber class]] || [minimized boolValue] ||
            !value || CFGetTypeID((__bridge CFTypeRef)value) != AXValueGetTypeID() ||
            !AXValueGetValue((__bridge AXValueRef)value, kAXValueCGSizeType, &size) ||
            !isfinite(size.width) || !isfinite(size.height) || size.width < 840 || size.height < 620 || size.width > 32768 || size.height > 32768 || !identity(NO)) continue;
        resultCode = 0;
        emit(@{@"protocol": @1, @"kind": @"packaged-desktop-main-window", @"pid": @(targetPid),
               @"executable": targetPath, @"startSec": @(startSec), @"startUsec": @(startUsec),
               @"role": role, @"subrole": subrole, @"title": textAttribute(element, kAXTitleAttribute),
               @"width": @(size.width), @"height": @(size.height), @"main": @YES, @"minimized": @NO});
        finished = YES;
        return;
    }
}

static void inspect(void) {
    if (mainWindowMode) { inspectMainWindow(); return; }
    if (finished || !identity(NO)) return;
    id windows = attribute(application, kAXWindowsAttribute);
    if (![windows isKindOfClass:[NSArray class]] || [windows count] > 16) return;
    for (id window in windows) {
        AXUIElementRef element = (__bridge AXUIElementRef)window;
        if (CFGetTypeID(element) != AXUIElementGetTypeID()) continue;
        NSString *role = textAttribute(element, kAXRoleAttribute);
        NSString *subrole = textAttribute(element, kAXSubroleAttribute);
        if (![role isEqualToString:@"AXDialog"] &&
            ![subrole isEqualToString:@"AXDialog"] && ![subrole isEqualToString:@"AXSystemDialog"]) continue;
        NSMutableArray *texts = [NSMutableArray array], *buttons = [NSMutableArray array];
        NSUInteger count = 0;
        if (!collect(element, 0, &count, texts, buttons)) continue;
        NSString *text = [texts componentsJoinedByString:@"\n"];
        NSString *refusal = [NSString stringWithFormat:@"Startup refused (%@).", code];
        if (text.length > 16384 ||
            ![texts containsObject:@"Jones Code could not start safely"] ||
            ![text containsString:refusal] ||
            (transactionId && ![text containsString:[@"Transaction: " stringByAppendingString:transactionId]]) ||
            buttons.count != 1 || !identity(NO)) continue;
        // Only this matched dialog's sole OK button is pressed; no global key event.
        AXUIElementRef button = (__bridge AXUIElementRef)buttons.firstObject;
        AXUIElementSetMessagingTimeout(button, 0.05f);
        AXError pressed = AXUIElementPerformAction(button, kAXPressAction);
        resultCode = pressed == kAXErrorSuccess ? 0 : 1;
        emit(@{@"protocol": @1, @"kind": @"packaged-desktop-native-dialog", @"pid": @(targetPid),
               @"executable": targetPath, @"startSec": @(startSec), @"startUsec": @(startUsec),
               @"code": code, @"transactionId": transactionId ?: [NSNull null],
               @"title": @"Jones Code could not start safely", @"text": text,
               @"role": role, @"subrole": subrole, @"dismissed": @(pressed == kAXErrorSuccess)});
        finished = YES;
        return;
    }
}

static void notified(AXObserverRef observer, AXUIElementRef element, CFStringRef notification, void *context) {
    inspect();
}

int main(int argc, const char **argv) {
    @autoreleasepool {
        mainWindowMode = argc == 5 && strcmp(argv[1], "--main-window") == 0;
        if (!mainWindowMode && argc != 6) return 2;
        int first = mainWindowMode ? 2 : 1;
        char *end = NULL;
        long number = strtol(argv[first], &end, 10);
        if (!end || *end || number <= 1 || number > INT_MAX || number == getpid()) return 2;
        targetPid = (pid_t)number;
        targetPath = [NSString stringWithUTF8String:argv[first + 1]];
        code = mainWindowMode ? nil : [NSString stringWithUTF8String:argv[3]];
        transactionId = mainWindowMode || strcmp(argv[4], "-") == 0 ? nil : [NSString stringWithUTF8String:argv[4]];
        const char *rootArgument = argv[mainWindowMode ? 4 : 5];
        NSString *root = [NSString stringWithUTF8String:rootArgument];
        struct stat rootInfo;
        char canonical[PATH_MAX];
        if (!realpath(rootArgument, canonical) || strcmp(canonical, rootArgument) ||
            lstat(rootArgument, &rootInfo) || !S_ISDIR(rootInfo.st_mode) || rootInfo.st_uid != getuid() ||
            (rootInfo.st_mode & 077) || ![targetPath hasPrefix:[root stringByAppendingString:@"/"]]) return 2;
        NSRegularExpression *codePattern = [NSRegularExpression regularExpressionWithPattern:@"^[a-z0-9-]{1,80}$" options:0 error:nil];
        NSRegularExpression *txPattern = [NSRegularExpression regularExpressionWithPattern:@"^[a-f0-9]{64}$" options:0 error:nil];
        if (!mainWindowMode && ([codePattern numberOfMatchesInString:code options:0 range:NSMakeRange(0, code.length)] != 1 ||
            (transactionId && [txPattern numberOfMatchesInString:transactionId options:0 range:NSMakeRange(0, transactionId.length)] != 1))) return 2;
        if (!AXIsProcessTrusted()) {
            emit(@{@"protocol": @1, @"status": @"unavailable", @"reason": @"accessibility-not-authorized"});
            return 77;
        }
        if (!identity(YES)) return 1;
        deadline = CFAbsoluteTimeGetCurrent() + 15.0;
        application = AXUIElementCreateApplication(targetPid);
        AXObserverRef observer = NULL;
        if (AXObserverCreate(targetPid, notified, &observer) != kAXErrorSuccess) {
            CFRelease(application);
            return 1;
        }
        CFRunLoopSourceRef source = AXObserverGetRunLoopSource(observer);
        CFRunLoopAddSource(CFRunLoopGetCurrent(), source, kCFRunLoopDefaultMode);
        AXError added = AXObserverAddNotification(observer, application, kAXWindowCreatedNotification, NULL);
        AXError mainAdded = mainWindowMode && added == kAXErrorSuccess
            ? AXObserverAddNotification(observer, application, kAXMainWindowChangedNotification, NULL) : kAXErrorSuccess;
        if (added != kAXErrorSuccess || mainAdded != kAXErrorSuccess) {
            emit(@{@"protocol": @1, @"status": @"unavailable", @"reason": @"window-observer-unavailable", @"axError": @(added != kAXErrorSuccess ? added : mainAdded)});
            resultCode = 77;
        } else {
            inspect();
            while (!finished && CFAbsoluteTimeGetCurrent() < deadline) {
                CFRunLoopRunInMode(kCFRunLoopDefaultMode, deadline - CFAbsoluteTimeGetCurrent(), true);
            }
            if (!finished) emit(@{@"protocol": @1, @"status": @"unproved",
                                 @"reason": mainWindowMode ? @"main-window-not-observed" : @"matching-dialog-not-observed"});
        }
        if (added == kAXErrorSuccess)
            AXObserverRemoveNotification(observer, application, kAXWindowCreatedNotification);
        if (mainWindowMode && mainAdded == kAXErrorSuccess)
            AXObserverRemoveNotification(observer, application, kAXMainWindowChangedNotification);
        CFRunLoopRemoveSource(CFRunLoopGetCurrent(), source, kCFRunLoopDefaultMode);
        CFRelease(observer);
        CFRelease(application);
        return resultCode;
    }
}
