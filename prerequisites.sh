mkdir -p dataset

count_file=`ls -1 ./dataset/*.csv 2>/dev/null | wc -l`

if [ $count_file -lt 2 ]
  then 
	rm -f ./dataset/*
	wget -nc http://files.grouplens.org/datasets/movielens/ml-25m.zip -P ./dataset/
	unzip -oj './dataset/ml-25m.zip' **/movies.csv **/ratings.csv -d ./dataset/
	rm ./dataset/*.zip
fi
