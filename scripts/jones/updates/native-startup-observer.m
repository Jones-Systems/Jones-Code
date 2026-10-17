#import <Foundation/Foundation.h>
#import <CoreServices/CoreServices.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void emit(NSDictionary *value) {
  NSData *data = [NSJSONSerialization dataWithJSONObject:value options:0 error:nil];
  flockfile(stdout);
  fwrite(data.bytes, 1, data.length, stdout);
  fputc('\n', stdout);
  fflush(stdout);
  funlockfile(stdout);
}

static void events(ConstFSEventStreamRef stream, void *context, size_t count,
                   void *paths, const FSEventStreamEventFlags flags[],
                   const FSEventStreamEventId ids[]) {
  (void)stream;
  (void)context;
  @autoreleasepool {
    char **names = paths;
    for (size_t index = 0; index < count; index++) {
      emit(@{@"type": @"event", @"path": [NSString stringWithUTF8String:names[index]],
             @"flags": @(flags[index]), @"id": [NSString stringWithFormat:@"%llu", (unsigned long long)ids[index]]});
    }
  }
}

int main(int argc, const char *argv[]) {
  @autoreleasepool {
    if (argc == 2 && strcmp(argv[1], "--paths") == 0) {
      struct passwd *account = getpwuid(getuid());
      if (account == NULL) return 2;
      emit(@{@"home": [NSHomeDirectory() stringByResolvingSymlinksInPath],
             @"accountHome": [[NSString stringWithUTF8String:account->pw_dir] stringByResolvingSymlinksInPath],
             @"applicationSupport": [NSSearchPathForDirectoriesInDomains(NSApplicationSupportDirectory, NSUserDomainMask, YES).firstObject stringByResolvingSymlinksInPath],
             @"caches": [NSSearchPathForDirectoriesInDomains(NSCachesDirectory, NSUserDomainMask, YES).firstObject stringByResolvingSymlinksInPath],
             @"uid": @(getuid())});
      return 0;
    }
    if (argc < 2) return 2;
    NSMutableArray *roots = [NSMutableArray array];
    for (int index = 1; index < argc; index++) [roots addObject:[NSString stringWithUTF8String:argv[index]]];
    FSEventStreamContext context = {0, NULL, NULL, NULL, NULL};
    FSEventStreamRef stream = FSEventStreamCreate(NULL, events, &context, (__bridge CFArrayRef)roots,
      kFSEventStreamEventIdSinceNow, 0.01,
      kFSEventStreamCreateFlagFileEvents | kFSEventStreamCreateFlagWatchRoot | kFSEventStreamCreateFlagNoDefer);
    if (stream == NULL) return 3;
    dispatch_queue_t queue = dispatch_queue_create("jones.native-startup-observer", DISPATCH_QUEUE_SERIAL);
    FSEventStreamSetDispatchQueue(stream, queue);
    if (!FSEventStreamStart(stream)) { FSEventStreamInvalidate(stream); FSEventStreamRelease(stream); return 4; }
    emit(@{@"type": @"ready"});
    char *line = NULL;
    size_t capacity = 0;
    while (getline(&line, &capacity, stdin) != -1) {
      if (strncmp(line, "flush:", 6) == 0) {
        // Flush from the control thread, never from the callback queue. The
        // acknowledgement follows callbacks for all mutations already made.
        FSEventStreamFlushSync(stream);
        dispatch_sync(queue, ^{});
        NSString *token = [[NSString stringWithUTF8String:line + 6] stringByTrimmingCharactersInSet:NSCharacterSet.whitespaceAndNewlineCharacterSet];
        emit(@{@"type": @"flushed", @"token": token});
      } else if (strcmp(line, "quit\n") == 0) break;
      else { free(line); return 5; }
    }
    free(line);
    FSEventStreamStop(stream);
    FSEventStreamInvalidate(stream);
    FSEventStreamRelease(stream);
    return 0;
  }
}
