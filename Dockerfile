FROM centos:7
COPY main /root/server
COPY ./data/test.txt /root/server
RUN chmod +x /root/server
EXPOSE 8080
CMD /root/server
