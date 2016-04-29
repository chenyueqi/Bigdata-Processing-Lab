本文件只针对第二次实验，并做为提交文件的一部分
## 文件结构
   ./report ........................................ 实验报告目录
	./report/report.pdf ........................ 实验报告
   ./src ........................................... 源代码目录
	./src/InvertedIndex ........................ 倒排表作业目录
	     ./src/InvertedIndex/InvertedIndex.jar.. 倒排表JAR包
	     ./src/InvertedIndex/*.java ............ 倒排表源代码文件
	./src/ResultSort ........................... 排序作业目录
             ./src/ResultSort/ResultSort.jar ....... 排序JAR包
	     ./src/ResultSort/*.java ............... 排序源代码文件
	./src/TFIDF ................................ TFIDF作业目录
	     ./src/TFIDF/TFIDF.jar ................. TFIDF JAR包
	     ./src/TFIDF/*.java .................... TFIDF源代码文件
    ./README.md .................................... 你正打开的

## 运行方式
  hadoop jar InvertedIndex.jar InvertedIndex [输入文件目录] [输出文件目录]
  hadoop jar ResultSort.jar ResultSort [输入文件目录] [输出文件目录]
  hadoop jar TFIDF.jar TFIDF [输入文件目录] [输出文件目录]

