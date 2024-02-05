FROM maven:3.9.2-eclipse-temurin-17 as build
COPY . /driver
RUN cd /driver && mvn --show-version --batch-mode clean install -P !determine-revision -DskipTests

FROM eclipse-temurin:17-jre
COPY --from=build /driver/benchkit-backend/target/benchkit-backend.jar /benchkit-backend.jar
CMD java -jar benchkit-backend.jar
