// Copyright (c) Meta Platforms, Inc. and affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/* External TTS for gadget replies (CONFIG_MUSE_TTS): an OpenAI-compatible
 * speech API (POST /v1/audio/speech, MP3 back), fetched on its own task and
 * handed to the chat session's MP3 decoder through a stream buffer. The key
 * is set on the console, like the SDK token; never committed. */
#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#if !CONFIG_MUSE_TTS

static inline bool muse_tts_console_command(const char *line) { (void)line; return false; }
static inline bool muse_tts_ready(void) { return false; }
static inline bool muse_tts_submit(const char *text) { (void)text; return false; }
static inline size_t muse_tts_poll(uint8_t *out, size_t cap) { (void)out; (void)cap; return 0; }
static inline void muse_tts_abort(void) {}
static inline void muse_tts_init(void) {}

typedef enum { MUSE_TTS_IDLE = 0 } muse_tts_status_t;
static inline muse_tts_status_t muse_tts_status(void) { return MUSE_TTS_IDLE; }

#else

#ifdef __cplusplus
extern "C" {
#endif

/* Console lines: ">tts_key=...", ">tts_url=...", ">tts_model=...",
 * ">tts_voice=..." and ">tts_show". Returns true when the line was one of
 * these; call from the console hook chain. */
bool muse_tts_console_command(const char *line);

/* Whether a key is set: without one, replies stay captions. */
bool muse_tts_ready(void);

/* Ask the fetch task to speak `text` (UTF-8; copied, up to 4 KB). Returns
 * false when not ready or the previous request is still winding down; the
 * caller then falls back to captions at reading pace. */
bool muse_tts_submit(const char *text);

/* Move fetched MP3 from the stream buffer into out; returns bytes moved. */
size_t muse_tts_poll(uint8_t *out, size_t cap);

typedef enum {
    MUSE_TTS_IDLE,     /* nothing in flight */
    MUSE_TTS_RUNNING,  /* fetching, or output still unread */
    MUSE_TTS_DONE,     /* the response is complete; read the rest and speak it */
    MUSE_TTS_FAILED,   /* the request failed; play what arrived, if anything */
} muse_tts_status_t;

muse_tts_status_t muse_tts_status(void);

/* Stop any in-flight request and drop its output. Safe from any task. */
void muse_tts_abort(void);

/* Allocate the handoff and load the config; the chat session's start calls it. */
void muse_tts_init(void);

#ifdef __cplusplus
}
#endif

#endif
