# N-gram
Making n-gram words by using hadoop


# Goal of Program

Hadoop을 사용하여 주어진 데이터를 이용하여 N-gram Count, RF만들기



N-gram Count

Map : 주어진 데이터를 N-gram 형태로 intermediate result 만들기 (Key : Each word in data , Value : N-gram words of key with count )

Reduce : Map의 output을 aggregate하기 (Key : N-gram words, Value : Total count )



N-gram RF(Relative Frequency)

Map : 주어진 데이터를 N-gram 형태로 intermediate result 만들기 (Key : Each word in data , Value : N-gram words of key with count ) + Key : "*" (각 단어마다 total n-gram단어를 구하기위한 용도)

Reduce : Map의 output을 aggregate 한 후 total count를 Key가 "*"인 total n-gram count로 나누어 output을 구하기



# Used language

Java


# My Review

