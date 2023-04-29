ffmpeg \
-f v4l2 \
-framerate 90 \
-re -stream_loop -1 \
-video_size 640x320 \
-input_format mjpeg \
-i /dev/video0 \
-c copy \
-f rtsp \
rtsp://localhost:8558/stream
