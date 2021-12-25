FROM centos:7
COPY main /root/server
RUN chmod +x /root/server \
    && mkdir /root/data \
    && mkdir /root/log
COPY ./data/* /root/data/
COPY ./log/* /root/log/
EXPOSE 8080
CMD cd /root && ./server
