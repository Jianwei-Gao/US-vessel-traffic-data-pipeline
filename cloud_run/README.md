# base docker image for cloud run job 
This is the code used to build the docker image on dockerhub that serves as the base image for cloud run 

if you wish to use your own docker image on docker hub as base image for cloud run, log into docker vai

```
docker login -u <username> -p <password>

docker build . -t \<username\>/\<image name\>:\<version\> &&
docker push \<username\>/\<image name\>:\<version\>
```

then in "main.tf" in terraform/wkdir/, replace 
```
image = "jianweigao/noaa_ais_data_ingestion_cloudrun_job"
```
with
```
image = "<username>/<image name>:<version>"
```