This document intends to start webcam on laptop for testing. Once mediamtx and start_cam.sh run successfully, eva can access webcam by 'LOAD WEBCAM'
install mediamtx from https://github.com/aler9/mediamtx . OFficial release works fine
setup mediamtx.yml with unused port for rtsp server.(configure rtspAddress field)  Run ./mediamtx then.
Run start_cam.sh on another process. Make sure port matches to the rtspAddress in mediamtx.yml
