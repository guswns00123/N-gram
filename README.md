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
아무래도 하둡을 처음 사용하다보니 굉장히 낯설고 어떻게 시스템이 돌아가는지 이해하는데 많은 시간을 소요했었다. 

하지만 Mapper에서 데이터를 나누어 가공하고 그 가공한 데이터를 sorting하여 reducer에서 정보를 종합하여 출력할 수 있는 시스템을 이해하고나서 이런식으로 big data를 처리할 수 있겠구나를 깨닫게되었다. 

그리고 이 과제를 제출했을 때 demo를 했는데 나 같은 경우는 주어진 data를 처리하는데 1분정도 걸렸는데 다른 친구의 알고리즘은 20초 쯤 걸리는 걸 보고 어떻게 데이터를 처리하냐에 따라 속도가 확연히 달라질 수 있구나를 알았다.

나중에 궁금해서 tutor한테 물어봤는데 mapper에서 reducer로 보낼때 최대한 적은 양(필요한 정보만!)의 data를 보내면 속도를 확실히 빠르게 할 수 있을 거라고 했다.
