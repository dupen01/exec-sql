import re


def xtrim(o_str):
    if o_str:
        return re.sub(r'[\s\n\r\t]', '', str(o_str))


s = """
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 's3://lakehouse/paimon/warehouse',
    'lock.enabled' = 'true',
    's3.endpoint' = 'http://172.20.3.12:9000',
    's3.access-key' = 'minio',
    's3.secret-key' = '12345678'
);

EXECUTE STATEMENT SET
BEGIN
insert into paimon.test.t2
select 
    id,
    name,
    date_format(ts,'yyyy-MM-dd')
from t1;
insert into paimon.test.t1
select * from paimon.test.t2;
END;

select * from t;
"""

if __name__ == '__main__':
    r = re.search(r'^EXECUTE STATEMENT.*END;$', s, flags=re.M | re.S | re.I)
    print(r.group())
