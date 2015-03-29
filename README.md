# twitterme
Twitter Page Rank using Hadoop Map Reduce
This is a simple map reduce program to calculate user ranks / page ranks
for twitter users..

Using a simple formula for page rank
i.e Page Rank(user) = N - O/N

N--> Total number of followers of a user
O--> Total number of outlinks from the followers of user

analyzed data set of 1.5 GB on a single node hadoop cluster under 15
minutes...

data set is available on
http://an.kaist.ac.kr/traces/WWW2010.html
