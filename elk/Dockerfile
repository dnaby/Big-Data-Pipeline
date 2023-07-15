
FROM docker.elastic.co/logstash/logstash:8.3.3

RUN logstash-plugin install logstash-codec-json
RUN rm -f /usr/share/logstash/pipeline/logstash.conf
COPY logstash.conf /usr/share/logstash/pipeline/

CMD logstash -f /usr/share/logstash/pipeline/logstash.conf --config.reload.automatic