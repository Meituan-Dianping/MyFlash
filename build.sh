yum install glib2-devel -y
gcc -g -w  `pkg-config --cflags --libs glib-2.0` source/binlogParseGlib.c  -o binary/flashback
