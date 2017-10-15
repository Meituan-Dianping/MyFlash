### 1.How to build

```
gcc -w  `pkg-config --cflags --libs glib-2.0` source/binlogParseGlib.c  -o binary/flashback
```
为了保证在一台机器上编译后，可以在其它机器上使用，需要满足以下两个条件
a) 将glib做成静态链接
b）在编译的那台机器的glibc版本（查看方法为ldd --version）要小于等于要运行该软件的那台机器glibc版本
因此需要你在一台glibc版本较低的机器上运行如下命令即可。
```
gcc -w -g `pkg-config --cflags  glib-2.0` source/binlogParseGlib.c   -o binary/flashback /usr/lib64/libglib-2.0.a -lrt
```
