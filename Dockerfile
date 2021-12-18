FROM centos:7
COPY main /root/server
RUN chmod +x /root/server
EXPOSE 8080
CMD /root/server
