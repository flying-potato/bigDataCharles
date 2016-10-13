A = LOAD 'input_for_pig' AS (line:chararray);
result1 = FILTER A BY (LOWER($0) matches '.*hackathon.*');
r1 = GROUP result1 ALL;
x1 = foreach r1 generate 'hackathon',COUNT_STAR($1);
STORE x1 INTO 'out1';

result2 = FILTER A BY (LOWER($0) matches '.*chicago.*');
r2 = GROUP result2 ALL;
x2 = foreach r2 generate 'chicago',COUNT_STAR($1);
STORE x2 INTO 'out2';

result3 = FILTER A BY (LOWER($0) matches '.*dec.*');
r3 = GROUP result3 ALL;
x3 = foreach r3 generate 'dec',COUNT_STAR($1);
STORE x3 INTO 'out3';

result4 = FILTER A BY (LOWER($0) matches '.*java.*');
r4 = GROUP result4 ALL;
x4 = foreach r4 generate 'java',COUNT_STAR($1);
STORE x4 INTO 'out4';

B1 = LOAD 'out1' AS (word:chararray, num:int);
B2 = LOAD 'out2' AS (word:chararray, num:int);
B3 = LOAD 'out3' AS (word:chararray, num:int);
B4 = LOAD 'out4' AS (word:chararray, num:int);

C = LOAD 'keywords' USING PigStorage(',') AS (word:chararray, num:int);

X = UNION B1,B2,B3,B4,C;
Y = GROUP X BY word;
Z = FOREACH Y GENERATE group, MAX(X.num);

STORE Z INTO 'out6';
