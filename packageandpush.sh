link="target/userbehavior-"
filetype=".jar"
if [ -z "$1" ]
	then
		echo "Need version"
		exit 1
fi
mvn package
path="${link}$1${filetype}"
scp -P 2395 $path cuonghn@10.3.14.162:/home/cuonghn/cache/c71
