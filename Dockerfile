FROM openjdk:17-buster

ENV MB_PLUGINS_DIR=/home/plugins/

ADD https://downloads.metabase.com/v0.47.0/metabase.jar /home
ADD target/greptimedb.metabase-driver.jar /home/plugins/

RUN chmod 744 /home/plugins/greptimedb.metabase-driver.jar

CMD ["java", "-jar", "/home/metabase.jar"]