## 计算共同好友（粉丝）

> 实验来源：[实战案例玩转Hadoop系列13-Map Reduce进阶编程案例（计算共同好友）](https://zhuanlan.zhihu.com/p/50236955)


### 运行步骤

```
# 在 HDFS 中创建存放输入文件夹
hdfs dfs -mkdir -p /hadoop/commonfans/input
# 将数据文件上传到 HDFS 输入文件夹中
hdfs dfs -copyFromLocal fans.txt /hadoop/commonfans/input

# 步骤一的结果存放在 /hadoop/commonfans/tmp
# 运行步骤一
hadoop jar common-fans-1.0-SNAPSHOT.jar job1.CommonFriendStep1 /hadoop/commonfans/input/fans.txt /hadoop/commonfans/tmp
# 查看步骤一运行结果
hdfs dfs -ls /hadoop/commonfans/tmp
hdfs dfs -cat /hadoop/commonfans/tmp/part-r-00000

# 运行步骤二
hadoop jar common-fans-1.0-SNAPSHOT.jar job2.CommonFriendStep2 /hadoop/commonfans/tmp/part-r-00000 /hadoop/commonfans/output
# 查看步骤二结果
hdfs dfs -ls /hadoop/commonfans/output
hdfs dfs -cat /hadoop/commonfans/output/part-r-00000
```

### 需求描述

某社交网站，有如下用户关系数据：

```
A:B,C,D,F,E,O
B:A,C,E,K
C:F,A,D,I
D:A,E,F,L
E:B,C,D,M,L
F:A,B,C,D,E,O,M
G:A,C,D,E,F
H:A,C,D,E,O
I:A,O
J:B,O
K:A,C,D
L:D,E,F
M:E,F,G
O:A,H,I,J
```

样例数据说明：

```
A:B,C,D,F,E,O
```

每行数据以冒号为分隔符：

* 冒号左边是网站的一个用户A；
* 冒号右边是用户A的粉丝列表（关注用户A的粉丝，用逗号隔开）；

现在，需要对网站的几十亿用户进行分析，找出哪些用户两两之间有共同的粉丝，以及他俩的共同粉丝都有哪些人。比如，A、B两个用户拥有共同粉丝C和E；


### 步骤一结果

```
B-C	A
B-D	A
B-F	A
B-G	A
B-H	A
B-I	A
B-K	A
B-O	A
C-D	A
C-F	A
C-G	A
C-H	A
C-I	A
C-K	A
C-O	A
D-F	A
D-G	A
D-H	A
D-I	A
D-K	A
D-O	A
F-G	A
F-H	A
F-I	A
F-K	A
F-O	A
G-H	A
G-I	A
G-K	A
G-O	A
H-I	A
H-K	A
H-O	A
I-K	A
I-O	A
K-O	A
A-E	B
A-F	B
A-J	B
E-F	B
E-J	B
F-J	B
A-B	C
A-E	C
A-F	C
A-G	C
A-H	C
A-K	C
B-E	C
B-F	C
B-G	C
B-H	C
B-K	C
E-F	C
E-G	C
E-H	C
E-K	C
F-G	C
F-H	C
F-K	C
G-H	C
G-K	C
H-K	C
A-C	D
A-E	D
A-F	D
A-G	D
A-H	D
A-K	D
A-L	D
C-E	D
C-F	D
C-G	D
C-H	D
C-K	D
C-L	D
E-F	D
E-G	D
E-H	D
E-K	D
E-L	D
F-G	D
F-H	D
F-K	D
F-L	D
G-H	D
G-K	D
G-L	D
H-K	D
H-L	D
K-L	D
A-B	E
A-D	E
A-F	E
A-G	E
A-H	E
A-L	E
A-M	E
B-D	E
B-F	E
B-G	E
B-H	E
B-L	E
B-M	E
D-F	E
D-G	E
D-H	E
D-L	E
D-M	E
F-G	E
F-H	E
F-L	E
F-M	E
G-H	E
G-L	E
G-M	E
H-L	E
H-M	E
L-M	E
A-C	F
A-D	F
A-G	F
A-L	F
A-M	F
C-D	F
C-G	F
C-L	F
C-M	F
D-G	F
D-L	F
D-M	F
G-L	F
G-M	F
L-M	F
C-O	I
D-E	L
E-F	M
A-F	O
A-H	O
A-I	O
A-J	O
F-H	O
F-I	O
F-J	O
H-I	O
H-J	O
I-J	O
```

### 步骤二运行结果

```
A-B	E,C
A-C	D,F
A-D	E,F
A-E	D,B,C
A-F	O,B,C,D,E
A-G	F,E,C,D
A-H	E,C,D,O
A-I	O
A-J	O,B
A-K	D,C
A-L	F,E,D
A-M	E,F
B-C	A
B-D	A,E
B-E	C
B-F	E,A,C
B-G	C,E,A
B-H	A,E,C
B-I	A
B-K	C,A
B-L	E
B-M	E
B-O	A
C-D	A,F
C-E	D
C-F	D,A
C-G	D,F,A
C-H	D,A
C-I	A
C-K	A,D
C-L	D,F
C-M	F
C-O	I,A
D-E	L
D-F	A,E
D-G	E,A,F
D-H	A,E
D-I	A
D-K	A
D-L	E,F
D-M	F,E
D-O	A
E-F	D,M,C,B
E-G	C,D
E-H	C,D
E-J	B
E-K	C,D
E-L	D
F-G	D,C,A,E
F-H	A,D,O,E,C
F-I	O,A
F-J	B,O
F-K	D,C,A
F-L	E,D
F-M	E
F-O	A
G-H	D,C,E,A
G-I	A
G-K	D,A,C
G-L	D,F,E
G-M	E,F
G-O	A
H-I	O,A
H-J	O
H-K	A,C,D
H-L	D,E
H-M	E
H-O	A
I-J	O
I-K	A
I-O	A
K-L	D
K-O	A
L-M	E,F
```
