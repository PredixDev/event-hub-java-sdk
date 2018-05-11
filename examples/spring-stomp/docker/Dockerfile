FROM java:8
ADD spring-stomp-1.0.0.jar /app/
ADD run-app.sh /app/
RUN ["chmod", "+x", "app/run-app.sh"]
CMD ["./app/run-app.sh", "-c"]
