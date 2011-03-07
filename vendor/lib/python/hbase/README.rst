This is generated code. Thrift is a ghetto.

curl 'http://svn.apache.org/viewvc/hbase/trunk/src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift?revision=946530&view=co' > hbase.thrift
thrift --gen py hbase.thrift
mv gen-py/hbase ~/dev/glow
