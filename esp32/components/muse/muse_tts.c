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

/* muse_tts.c: speaks gadget replies through an OpenAI-compatible speech API.
 *
 * The chat session (session task) calls muse_tts_submit() with a reply's text
 * and polls muse_tts_poll() / muse_tts_status() on its loop; a fetch task of
 * our own does the HTTPS request (esp_http_client, MP3 response) and feeds a
 * stream buffer, backpressured when the session's decode can't keep up. The
 * API key lives in NVS, set from the console like the SDK token; nothing is
 * committed anywhere. muse_tts_init() runs once from the chat session start. */

#include "muse_tts.h"

#include <stdatomic.h>
#include <string.h>

#include "cJSON.h"
#include "esp_crt_bundle.h"
#include "esp_heap_caps.h"
#include "esp_http_client.h"
#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/stream_buffer.h"
#include "freertos/task.h"
#include "nvs.h"

static const char *TAG = "muse.tts";

#define TEXT_CAP 4096                 /* request input cap, UTF-8 bytes */
#define CHUNK 4096                    /* both the read size and the poll size */
#define STREAM_BYTES (192 * 1024)     /* MP3 handoff; decode drains the rest */
#define HTTP_TIMEOUT_MS 15000         /* per read/connect; the server can stall */
#define SUBMIT_WAIT_MS 3000           /* for a previous request to wind down */

typedef enum {
    CFG_URL, CFG_MODEL, CFG_VOICE, CFG_KEY, CFG_N,
} cfg_t;

static const char *CFG_NAMES[CFG_N] = { "url", "model", "voice", "key" };
static const char *CFG_DEFAULTS[CFG_N] = {
    CONFIG_MUSE_TTS_URL, CONFIG_MUSE_TTS_MODEL, CONFIG_MUSE_TTS_VOICE, "",
};

static char s_cfg[CFG_N][128];
static SemaphoreHandle_t s_lock;
static StreamBufferHandle_t s_stream;
static atomic_int s_status = MUSE_TTS_IDLE;
static atomic_bool s_abort = false;
static atomic_bool s_api_error = false;   /* the provider answered, but with an error */

/* ---- config ---- */

static void cfg_load(void) {
    for (int i = 0; i < CFG_N; i++) {
        strlcpy(s_cfg[i], CFG_DEFAULTS[i], sizeof(s_cfg[i]));
    }
    nvs_handle_t h;
    if (nvs_open("muse_tts", NVS_READONLY, &h) != ESP_OK) {
        return;
    }
    for (int i = 0; i < CFG_N; i++) {
        size_t len = sizeof(s_cfg[i]);
        nvs_get_str(h, CFG_NAMES[i], s_cfg[i], &len);
    }
    nvs_close(h);
}

static bool cfg_store(cfg_t which, const char *value) {
    if (!*value || strlen(value) >= sizeof(s_cfg[0])) {
        return false;
    }
    nvs_handle_t h;
    if (nvs_open("muse_tts", NVS_READWRITE, &h) != ESP_OK) {
        return false;
    }
    bool ok = nvs_set_str(h, CFG_NAMES[which], value) == ESP_OK && nvs_commit(h) == ESP_OK;
    nvs_close(h);
    if (ok) {
        xSemaphoreTake(s_lock, portMAX_DELAY);
        strlcpy(s_cfg[which], value, sizeof(s_cfg[which]));
        xSemaphoreGive(s_lock);
    }
    return ok;
}

/* ---- the fetch task ---- */

typedef struct {
    char *text;                           /* the reply's text, owned here */
    char url[128], key[128], model[128], voice[128];
} fetch_t;

static void set_status(muse_tts_status_t st) { atomic_store(&s_status, st); }

static void fetch_fail(void) {
    set_status(MUSE_TTS_FAILED);
    vTaskDelete(NULL);
}

/* MiniMax's t2a_v2 isn't OpenAI-shaped: hex-encoded MP3 chunks ride inside
 * SSE "data:" lines, so the URL gives the provider away. */
static bool provider_is_minimax(const char *url) {
    return strstr(url, "minimax") != NULL;
}

static int hexval(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/* One line of MiniMax's SSE stream: {"data":{"audio":"<hex>","status":1}}.
 * Decodes the hex in place and hands the MP3 bytes to the handoff buffer.
 * Sets saw_audio when audio arrived; a base_resp error with none fails. */
static void tts_sse_line(char *line, bool *saw_audio) {
    char *p = line;
    if (!strncmp(p, "data:", 5)) p += 5;
    while (*p == ' ') p++;
    char *audio = strstr(p, "\"audio\":\"");
    if (audio) {
        audio += 9;
        char *end = strchr(audio, '"');
        if (!end) {
            return;
        }
        size_t hexn = (size_t)(end - audio), bn = 0;
        for (size_t i = 0; i + 1 < hexn; i += 2) {
            int hi = hexval(audio[i]), lo = hexval(audio[i + 1]);
            if (hi < 0 || lo < 0) {
                return;
            }
            audio[bn++] = (char)((hi << 4) | lo);   /* out never overruns in */
        }
        size_t at = 0;
        while (at < bn && !atomic_load(&s_abort)) {
            size_t put = xStreamBufferSend(s_stream, audio + at, bn - at, pdMS_TO_TICKS(200));
            at += put;
        }
        *saw_audio = true;
        return;
    }
    const char *err = strstr(p, "\"status_code\":");
    if (err) {
        long code = strtol(err + 14, NULL, 10);
        if (code) {
            ESP_LOGW(TAG, "MiniMax answered status_code %ld", code);
            atomic_store(&s_api_error, true);
        }
    }
}

/* MiniMax's response: SSE lines carrying hex audio chunks. Body bytes are
 * buffered until a newline closes a line, then parsed; the tail is parsed
 * when the response ends. */
static void fetch_minimax_body(esp_http_client_handle_t http, uint8_t *buf, bool *saw_audio) {
    enum { LINE_CAP = 32 * 1024 };
    char *line = heap_caps_malloc(LINE_CAP, MALLOC_CAP_8BIT | MALLOC_CAP_SPIRAM);
    if (!line) {
        return;
    }
    size_t len = 0;
    while (!atomic_load(&s_abort)) {
        int n = esp_http_client_read(http, (char *)buf, CHUNK);
        if (n < 0) {
            break;
        }
        if (!n) {
            if (esp_http_client_is_complete_data_received(http)) {
                break;
            }
            continue;   /* a stall; the read timeout bounds the wait */
        }
        for (int i = 0; i < n; i++) {
            if (len < LINE_CAP - 1) {
                line[len++] = buf[i];
            }
            if (buf[i] != '\n') {
                continue;
            }
            line[len] = '\0';
            len = 0;
            tts_sse_line(line, saw_audio);
        }
    }
    if (len && len < LINE_CAP) {
        line[len] = '\0';
        tts_sse_line(line, saw_audio);
    }
    heap_caps_free(line);
}

static void fetch_run(fetch_t *f) {
    esp_http_client_config_t cfg = {
        .url = f->url,
        .timeout_ms = HTTP_TIMEOUT_MS,
        .buffer_size = CHUNK,
        .buffer_size_tx = 2048,
        .crt_bundle_attach = esp_crt_bundle_attach,
        .method = HTTP_METHOD_POST,
    };
    esp_http_client_handle_t http = esp_http_client_init(&cfg);
    esp_err_t err = http ? ESP_OK : ESP_FAIL;
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "couldn't start the HTTP client");
    }

    char auth[8 + sizeof(f->key)];
    char *json = NULL;
    bool minimax = provider_is_minimax(f->url);
    if (err == ESP_OK) {
        snprintf(auth, sizeof(auth), "Bearer %s", f->key);
        cJSON *body = cJSON_CreateObject();
        if (body) {
            cJSON_AddStringToObject(body, "model", f->model);
            cJSON_AddStringToObject(body, "text", f->text);
            if (minimax) {
                cJSON_AddBoolToObject(body, "stream", true);
                cJSON *vs = cJSON_AddObjectToObject(body, "voice_setting");
                cJSON_AddStringToObject(vs, "voice_id", f->voice);
                cJSON_AddNumberToObject(vs, "speed", 1.0);
                cJSON_AddNumberToObject(vs, "vol", 1.0);
                cJSON_AddNumberToObject(vs, "pitch", 0);
                cJSON *as = cJSON_AddObjectToObject(body, "audio_setting");
                cJSON_AddNumberToObject(as, "sample_rate", 32000);
                cJSON_AddStringToObject(as, "format", "mp3");
                cJSON_AddNumberToObject(as, "channel", 1);
            } else {
                cJSON_AddStringToObject(body, "voice", f->voice);
                cJSON_AddStringToObject(body, "response_format", "mp3");
            }
            json = cJSON_PrintUnformatted(body);
            cJSON_Delete(body);
        }
        if (!json) {
            ESP_LOGE(TAG, "out of memory for the request body");
            err = ESP_FAIL;
        }
    }

    if (err == ESP_OK) {
        err = esp_http_client_open(http, (int)strlen(json));
        int sent = 0;
        while (err == ESP_OK && sent < (int)strlen(json) && !atomic_load(&s_abort)) {
            int n = esp_http_client_write(http, json + sent, (int)strlen(json) - sent);
            if (n <= 0) {
                err = ESP_FAIL;
            } else {
                sent += n;
            }
        }
        if (err == ESP_OK && !atomic_load(&s_abort)) {
            if (esp_http_client_fetch_headers(http) < 0 ||
                esp_http_client_get_status_code(http) != 200) {
                ESP_LOGW(TAG, "TTS API answered %d", esp_http_client_get_status_code(http));
                err = ESP_FAIL;
            }
        }
    }
    cJSON_free(json);
    bool saw_audio = false;
    atomic_store(&s_api_error, false);
    if (err == ESP_OK && !atomic_load(&s_abort)) {
        /* Response body: MP3 bytes, streamed into the handoff buffer. A busy
         * decode backs this up, so keep trying to send while not aborted. */
        uint8_t *buf = heap_caps_malloc(CHUNK, MALLOC_CAP_8BIT);
        if (!buf) {
            err = ESP_FAIL;
        } else {
            if (minimax) {
                fetch_minimax_body(http, buf, &saw_audio);
            } else {
                while (!atomic_load(&s_abort)) {
                    int n = esp_http_client_read(http, (char *)buf, CHUNK);
                    if (n < 0) {
                        err = ESP_FAIL;
                        break;
                    }
                    if (!n) {
                        if (esp_http_client_is_complete_data_received(http)) {
                            break;
                        }
                        continue;   /* a stall; the read timeout bounds the wait */
                    }
                    size_t at = 0;
                    while (at < (size_t)n && !atomic_load(&s_abort)) {
                        size_t put = xStreamBufferSend(s_stream, buf + at, n - at, pdMS_TO_TICKS(200));
                        at += put;
                    }
                    saw_audio = true;
                }
            }
            heap_caps_free(buf);
        }
    }
    esp_http_client_cleanup(http);
    heap_caps_free(f->text);
    heap_caps_free(f);
    if (atomic_load(&s_abort)) {
        set_status(MUSE_TTS_IDLE);   /* aborted: the caller moved on */
    } else if (err != ESP_OK || (minimax && !saw_audio) ||
               (!minimax && atomic_load(&s_api_error))) {
        fetch_fail();
        return;
    } else {
        set_status(MUSE_TTS_DONE);
    }
    vTaskDelete(NULL);
}

/* ---- public API ---- */

bool muse_tts_ready(void) {
    if (!s_lock) {
        return false;
    }
    xSemaphoreTake(s_lock, portMAX_DELAY);
    bool ready = s_cfg[CFG_KEY][0] != '\0';
    xSemaphoreGive(s_lock);
    return ready;
}

bool muse_tts_submit(const char *text) {
    if (!s_lock || !s_stream) {
        return false;
    }
    int wait_ms = SUBMIT_WAIT_MS;
    /* A previous request still RUNNING (say, stuck in a read): ask it to stop
     * and give it a moment before reusing the handoff buffer. DONE and FAILED
     * need no wait: their task is already gone. */
    if (atomic_load(&s_status) == MUSE_TTS_RUNNING) {
        atomic_store(&s_abort, true);
        while (atomic_load(&s_status) == MUSE_TTS_RUNNING) {
            if (!(wait_ms -= 50)) {
                ESP_LOGW(TAG, "TTS task busy; captions only");
                return false;
            }
            vTaskDelay(pdMS_TO_TICKS(50));
        }
    }
    xSemaphoreTake(s_lock, portMAX_DELAY);
    fetch_t *f = heap_caps_malloc(sizeof(fetch_t), MALLOC_CAP_8BIT);
    if (!f) {
        xSemaphoreGive(s_lock);
        return false;
    }
    strlcpy(f->url, s_cfg[CFG_URL], sizeof(f->url));
    strlcpy(f->key, s_cfg[CFG_KEY], sizeof(f->key));
    strlcpy(f->model, s_cfg[CFG_MODEL], sizeof(f->model));
    strlcpy(f->voice, s_cfg[CFG_VOICE], sizeof(f->voice));
    xSemaphoreGive(s_lock);
    if (!f->key[0]) {
        heap_caps_free(f);
        return false;
    }

    size_t len = strlen(text);
    bool cut = false;
    if (len > TEXT_CAP) {
        /* Cut on a UTF-8 boundary so no half character goes to the API. */
        len = TEXT_CAP;
        while (len && (text[len] & 0xc0) == 0x80) len--;
        cut = true;
    }
    f->text = heap_caps_malloc(len + 1, MALLOC_CAP_8BIT);
    if (!f->text) {
        heap_caps_free(f);
        ESP_LOGE(TAG, "out of memory for the TTS text");
        return false;
    }
    memcpy(f->text, text, len);
    f->text[len] = '\0';

    xStreamBufferReset(s_stream);
    atomic_store(&s_abort, false);
    set_status(MUSE_TTS_RUNNING);
    if (xTaskCreate((TaskFunction_t)fetch_run, "muse_tts", 8192, f, 5, NULL) != pdTRUE) {
        heap_caps_free(f->text);
        heap_caps_free(f);
        set_status(MUSE_TTS_IDLE);
        return false;
    }
    ESP_LOGI(TAG, "speaking %u bytes%s", (unsigned)len, cut ? " (cut)" : "");
    return true;
}

size_t muse_tts_poll(uint8_t *out, size_t cap) {
    if (!s_stream) {
        return 0;
    }
    return xStreamBufferReceive(s_stream, out, cap, 0);
}

muse_tts_status_t muse_tts_status(void) { return atomic_load(&s_status); }

void muse_tts_abort(void) { atomic_store(&s_abort, true); }

/* ---- console ---- */

bool muse_tts_console_command(const char *line) {
    static const struct { const char *name; cfg_t which; } cmds[] = {
        { "tts_key=", CFG_KEY }, { "tts_url=", CFG_URL },
        { "tts_model=", CFG_MODEL }, { "tts_voice=", CFG_VOICE },
    };
    for (size_t i = 0; i < sizeof(cmds) / sizeof(cmds[0]); i++) {
        size_t n = strlen(cmds[i].name);
        if (strncmp(line, cmds[i].name, n) != 0) {
            continue;
        }
        bool ok = s_lock && cfg_store(cmds[i].which, line + n);
        printf("@tts %s\n", ok ? "ok" : "error");
        fflush(stdout);
        return true;
    }
    if (!strcmp(line, "tts_show")) {
        if (!s_lock) {
            return true;
        }
        xSemaphoreTake(s_lock, portMAX_DELAY);
        char key[16];
        snprintf(key, sizeof(key), "%.8s…", s_cfg[CFG_KEY]);
        printf("@tts url=%s model=%s voice=%s key=%s%s\n",
               s_cfg[CFG_URL], s_cfg[CFG_MODEL], s_cfg[CFG_VOICE],
               s_cfg[CFG_KEY][0] ? key : "(unset)",
               s_cfg[CFG_KEY][0] ? "" : " (captions only)");
        xSemaphoreGive(s_lock);
        return true;
    }
    return false;
}

/* ---- startup, from the chat session start ---- */

void muse_tts_init(void) {
    s_lock = xSemaphoreCreateMutex();
    s_stream = xStreamBufferCreateWithCaps(STREAM_BYTES, 1, MALLOC_CAP_SPIRAM);
    if (!s_lock || !s_stream) {
        ESP_LOGE(TAG, "couldn't allocate the TTS handoff");
    }
    cfg_load();
}
