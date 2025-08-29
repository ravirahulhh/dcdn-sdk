#include <SDL2/SDL.h>
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/imgutils.h>
#include <libswscale/swscale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// 模拟输入数据（这里直接用本地文件）
typedef struct
{
    FILE* fp;
} MockData;

// 读取回调（替代 ffmpeg 默认的 fread）
int read_packet(void* opaque, uint8_t* buf, int buf_size)
{
    MockData* data = (MockData*)opaque;
    int ret = (int)fread(buf, 1, buf_size, data->fp);

    if (ret == 0) {
        // EOF
        return AVERROR_EOF;
    }

    // 如需模拟网络延迟，可取消注释
    // usleep(10 * 1000); // 10ms
    return ret;
}

// 修正后的 seek 回调：返回新的绝对位置；正确处理 AVSEEK_SIZE 和 AVSEEK_FORCE（用低 2 位）
int64_t seek(void* opaque, int64_t offset, int whence)
{
    MockData* data = (MockData*)opaque;

    if (whence == AVSEEK_SIZE) {
        off_t cur = ftello(data->fp);
        if (fseeko(data->fp, 0, SEEK_END) != 0) {
            return -1;
        }
        off_t size = ftello(data->fp);
        // 还原文件指针位置
        fseeko(data->fp, cur, SEEK_SET);
        return (int64_t)size;
    }

    int c_whence = whence & 0x3; // 0:SET 1:CUR 2:END
    if (fseeko(data->fp, (off_t)offset, c_whence) != 0) {
        return -1;
    }
    off_t pos = ftello(data->fp);
    return (int64_t)pos;
}

int main(int argc, char* argv[])
{
    // 关闭缓冲，保证日志即时输出
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);

    if (argc < 2) {
        printf("Usage: %s <input.mp4>\n", argv[0]);
        return -1;
    }

    av_log_set_level(AV_LOG_INFO);
    avformat_network_init();

    MockData mdata;
    mdata.fp = fopen(argv[1], "rb");
    if (!mdata.fp) {
        perror("fopen");
        return -1;
    }
    fprintf(stderr, "[player] opened file: %s\n", argv[1]);

    unsigned char* buffer = (unsigned char*)av_malloc(32 * 1024);
    AVIOContext* avio_ctx = avio_alloc_context(buffer, 32 * 1024, 0, &mdata, read_packet, NULL, seek);
    // 标记为可 seek
    if (avio_ctx) {
        avio_ctx->seekable = AVIO_SEEKABLE_NORMAL | AVIO_SEEKABLE_TIME;
    }

    AVFormatContext* fmt_ctx = avformat_alloc_context();
    fmt_ctx->pb = avio_ctx;
    // 告知使用自定义 IO
    fmt_ctx->flags |= AVFMT_FLAG_CUSTOM_IO;

    // 限制探测/分析时间，避免长时间卡住
    AVDictionary* opts = NULL;
    av_dict_set(&opts, "probesize", "1M", 0);
    av_dict_set(&opts, "analyzeduration", "1M", 0);

    fprintf(stderr, "[player] opening input...\n");
    if (avformat_open_input(&fmt_ctx, NULL, NULL, &opts) < 0) {
        fprintf(stderr, "[player] Could not open input\n");
        if (avio_ctx) {
            av_freep(&avio_ctx->buffer);
            avio_context_free(&avio_ctx);
        }
        fclose(mdata.fp);
        av_dict_free(&opts);
        return -1;
    }
    av_dict_free(&opts);
    fprintf(stderr, "[player] open_input ok\n");

    fprintf(stderr, "[player] finding stream info...\n");
    if (avformat_find_stream_info(fmt_ctx, NULL) < 0) {
        fprintf(stderr, "[player] find_stream_info failed\n");
        // 清理
        avformat_close_input(&fmt_ctx);
        if (avio_ctx) {
            av_freep(&avio_ctx->buffer);
            avio_context_free(&avio_ctx);
        }
        fclose(mdata.fp);
        return -1;
    }
    fprintf(stderr, "[player] find_stream_info ok: streams=%d\n", fmt_ctx->nb_streams);

    int video_stream_idx = -1;
    for (int i = 0; i < fmt_ctx->nb_streams; i++) {
        if (fmt_ctx->streams[i]->codecpar->codec_type == AVMEDIA_TYPE_VIDEO) {
            video_stream_idx = i;
            break;
        }
    }
    if (video_stream_idx == -1) {
        fprintf(stderr, "[player] No video stream found\n");
        if (avio_ctx) {
            av_freep(&avio_ctx->buffer);
            avio_context_free(&avio_ctx);
        }
        fclose(mdata.fp);
        return -1;
    }

    AVCodecParameters* codecpar = fmt_ctx->streams[video_stream_idx]->codecpar;
    const AVCodec* codec = avcodec_find_decoder(codecpar->codec_id);
    if (!codec) {
        fprintf(stderr, "[player] decoder not found for codec_id=%d\n", codecpar->codec_id);
        avformat_close_input(&fmt_ctx);
        if (avio_ctx) {
            av_freep(&avio_ctx->buffer);
            avio_context_free(&avio_ctx);
        }
        fclose(mdata.fp);
        return -1;
    }
    AVCodecContext* codec_ctx = avcodec_alloc_context3(codec);
    avcodec_parameters_to_context(codec_ctx, codecpar);
    if (avcodec_open2(codec_ctx, codec, NULL) < 0) {
        fprintf(stderr, "[player] open codec failed\n");
        avcodec_free_context(&codec_ctx);
        avformat_close_input(&fmt_ctx);
        if (avio_ctx) {
            av_freep(&avio_ctx->buffer);
            avio_context_free(&avio_ctx);
        }
        fclose(mdata.fp);
        return -1;
    }
    fprintf(
        stderr,
        "[player] video stream #%d %dx%d pix_fmt=%d\n",
        video_stream_idx,
        codec_ctx->width,
        codec_ctx->height,
        codec_ctx->pix_fmt);

    // SDL 初始化
    fprintf(stderr, "[player] init SDL...\n");
    if (SDL_Init(SDL_INIT_VIDEO) != 0) {
        fprintf(stderr, "[player] SDL_Init error: %s\n", SDL_GetError());
        // 清理
        avcodec_free_context(&codec_ctx);
        avformat_close_input(&fmt_ctx);
        if (avio_ctx) {
            av_freep(&avio_ctx->buffer);
            avio_context_free(&avio_ctx);
        }
        fclose(mdata.fp);
        return -1;
    }

    SDL_Window* win = SDL_CreateWindow(
        "FFmpeg Mock Player",
        SDL_WINDOWPOS_UNDEFINED,
        SDL_WINDOWPOS_UNDEFINED,
        codec_ctx->width,
        codec_ctx->height,
        SDL_WINDOW_SHOWN);
    if (!win) {
        fprintf(stderr, "[player] SDL_CreateWindow error: %s\n", SDL_GetError());
        SDL_Quit();
        avcodec_free_context(&codec_ctx);
        avformat_close_input(&fmt_ctx);
        if (avio_ctx) {
            av_freep(&avio_ctx->buffer);
            avio_context_free(&avio_ctx);
        }
        fclose(mdata.fp);
        return -1;
    }

    SDL_Renderer* renderer = SDL_CreateRenderer(win, -1, SDL_RENDERER_ACCELERATED | SDL_RENDERER_PRESENTVSYNC);
    if (!renderer) {
        fprintf(stderr, "[player] SDL_CreateRenderer error: %s\n", SDL_GetError());
        SDL_DestroyWindow(win);
        SDL_Quit();
        avcodec_free_context(&codec_ctx);
        avformat_close_input(&fmt_ctx);
        if (avio_ctx) {
            av_freep(&avio_ctx->buffer);
            avio_context_free(&avio_ctx);
        }
        fclose(mdata.fp);
        return -1;
    }

    SDL_Texture* texture = SDL_CreateTexture(
        renderer, SDL_PIXELFORMAT_IYUV, SDL_TEXTUREACCESS_STREAMING, codec_ctx->width, codec_ctx->height);
    if (!texture) {
        fprintf(stderr, "[player] SDL_CreateTexture error: %s\n", SDL_GetError());
        SDL_DestroyRenderer(renderer);
        SDL_DestroyWindow(win);
        SDL_Quit();
        avcodec_free_context(&codec_ctx);
        avformat_close_input(&fmt_ctx);
        if (avio_ctx) {
            av_freep(&avio_ctx->buffer);
            avio_context_free(&avio_ctx);
        }
        fclose(mdata.fp);
        return -1;
    }

    struct SwsContext* sws_ctx = sws_getContext(
        codec_ctx->width,
        codec_ctx->height,
        codec_ctx->pix_fmt,
        codec_ctx->width,
        codec_ctx->height,
        AV_PIX_FMT_YUV420P,
        SWS_BILINEAR,
        NULL,
        NULL,
        NULL);

    AVPacket* pkt = av_packet_alloc();
    AVFrame* frame = av_frame_alloc();
    AVFrame* frame_yuv = av_frame_alloc();

    int num_bytes = av_image_get_buffer_size(AV_PIX_FMT_YUV420P, codec_ctx->width, codec_ctx->height, 1);
    uint8_t* frame_buffer = (uint8_t*)av_malloc(num_bytes);
    av_image_fill_arrays(
        frame_yuv->data, frame_yuv->linesize, frame_buffer, AV_PIX_FMT_YUV420P, codec_ctx->width, codec_ctx->height, 1);

    fprintf(stderr, "[player] start reading frames...\n");
    int64_t frame_count = 0;
    int running = 1;
    while (running) {
        // 处理 SDL 事件，保证窗口响应 & 支持退出
        SDL_Event e;
        while (SDL_PollEvent(&e)) {
            if (e.type == SDL_QUIT) {
                running = 0;
            }
        }

        if (av_read_frame(fmt_ctx, pkt) < 0) {
            // EOF，开始冲刷解码器
            avcodec_send_packet(codec_ctx, NULL);
            while (avcodec_receive_frame(codec_ctx, frame) == 0) {
                sws_scale(
                    sws_ctx,
                    (const uint8_t* const*)frame->data,
                    frame->linesize,
                    0,
                    codec_ctx->height,
                    frame_yuv->data,
                    frame_yuv->linesize);

                SDL_UpdateYUVTexture(
                    texture,
                    NULL,
                    frame_yuv->data[0],
                    frame_yuv->linesize[0],
                    frame_yuv->data[1],
                    frame_yuv->linesize[1],
                    frame_yuv->data[2],
                    frame_yuv->linesize[2]);

                SDL_RenderClear(renderer);
                SDL_RenderCopy(renderer, texture, NULL, NULL);
                SDL_RenderPresent(renderer);
                frame_count++;
            }
            break;
        }

        if (pkt->stream_index == video_stream_idx) {
            if (avcodec_send_packet(codec_ctx, pkt) == 0) {
                while (avcodec_receive_frame(codec_ctx, frame) == 0) {
                    sws_scale(
                        sws_ctx,
                        (const uint8_t* const*)frame->data,
                        frame->linesize,
                        0,
                        codec_ctx->height,
                        frame_yuv->data,
                        frame_yuv->linesize);

                    SDL_UpdateYUVTexture(
                        texture,
                        NULL,
                        frame_yuv->data[0],
                        frame_yuv->linesize[0],
                        frame_yuv->data[1],
                        frame_yuv->linesize[1],
                        frame_yuv->data[2],
                        frame_yuv->linesize[2]);

                    SDL_RenderClear(renderer);
                    SDL_RenderCopy(renderer, texture, NULL, NULL);
                    SDL_RenderPresent(renderer);

                    frame_count++;
                    if ((frame_count % 30) == 0) {
                        fprintf(stderr, "[player] rendered %lld frames\n", (long long)frame_count);
                    }
                }
            }
        }
        av_packet_unref(pkt);
        SDL_Delay(1); // 略微让出 CPU
    }

    fprintf(stderr, "[player] done, total frames=%lld\n", (long long)frame_count);

    fclose(mdata.fp);
    av_frame_free(&frame);
    av_frame_free(&frame_yuv);
    avcodec_free_context(&codec_ctx);
    avformat_close_input(&fmt_ctx);
    if (avio_ctx) {
        av_freep(&avio_ctx->buffer);
        avio_context_free(&avio_ctx);
    }
    if (frame_buffer)
        av_free(frame_buffer);
    SDL_DestroyTexture(texture);
    SDL_DestroyRenderer(renderer);
    SDL_DestroyWindow(win);
    SDL_Quit();
    return 0;
}
