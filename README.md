# orcfile_convert
useage:  ./filehandle [OPTIONS]
必填参数：
	1. -i input文件名
	2. -o output文件名
	3. --inputFileFormat input 文件类型 目前支持orcfile，txtfile
	4. --outputFileFormat output文件类型 目前支持orcfile
	5. --table 输出文件对应的hive表名， 格式db.table， 用于自动获取orcfile列类型信息
	5. -s txtfile 转义符， 可选参数， 默认为\t


