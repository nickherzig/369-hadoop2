Nick Herzig
CSC 369 Lab 3
Winter 2023

Question 1:
The first order of buisiness for Q1 was to preform a reduce side join between Countries
and access log entries. I chose reduce side because there was already some examples for it,
and I felt it was a good first step into practing joining.
This was done by using the host name as the key for the mapping stage. 
The reduce stage than took each value, scraped for either an A or B, and depending on that 
added it to the messages list or countries list. Then the country that was found was emitted with each message.

These were then counted by country in the second mapreduce phase. This was done by making Country the key
and "1" the value. The reducer phase then summed all the intones up to emit the count.

To sort from high to low, a sorting comparator had to be used as the shuffling stage typical sorts by ascending order.
I used the counts as the key, and the my sorting comparator returned the opposite of the natural ascending sort.
When the reducer receieved the counts and their respective countries, they were in descending order.
So the reducer just emmitted all the countries for each count on their own line.

Question 2:
Since I was still connecting Countries with Access Log entries, I used the join map reduce that I described from
the first process of Question 1.

The second Map Reduce process was getting the count for each Country+URL instance. To do this, I had to create a new 
Writable object that represents a Country and URL pair. This is then used as the Key/Ouput for the mapper function. The value is
an intone. Reduce then adds up each instance of the pair, reports the pair and then its count.

To sort these values by country first, and then count in descending order, I had to create another object named CountryCountPair.
This CountryCount pair is used as the key for the final mapper, which is then sent to a SortComparator function. The sort comparator
uses the Countrycountpair compareto function to sort byu country first, then count in descending order. Reduce then takes in each
pair as the key and its count as the value, and then reports it in the order given by the sort comparator.

Question 3:
Since I already had unique Countries and Url pairings from Question 2, I used that as my jumping point for this question.
When creating the Key for the mapping function, I used the Country Url pair container I created in question 2.
This is done so a secondary sort and grouping can be done to order by Url, then country.
Value is country so they can be aggregated together later on.

The grouping comparator groups all the entries by Url, so all the countries are aggreagted in the reducer phase.
Sorting comparator sorts by Url first and then Country.

The input to reducer is a Url pair, and then the values are all the countries that were combined in the grouping phase.