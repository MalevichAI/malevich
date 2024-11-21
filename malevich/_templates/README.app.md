# Malevich App

Welcome to Malevich! You have just created a new Malevich App. The folder `./apps` contains Malevich-specific code, but
you are free to rearrange the code as you wish. Just keep in mind, that whatever objects (processors, inits, conditions and etc.) you
create should be copied to `./apps` in Docker image.

## How to Build

To build an app, run the following command:

```bash
docker build -t <IMAGE_NAME> .
```

To run your apps on Malevich Core, you need to make the image accessible. You can do it by pushing it to one of docker registries ([Dockerhub](https://hub.docker.com/), [Github CR](https://github.com/features/packages) and so on). Once 
your image is available, you may install it with the following command:

```bash
malevich use {app} <IMAGE_NAME>
```

In case you pushed to a private registry, you need to provide credentials:

```bash
malevich use {app} <IMAGE_NAME> <USERNAME> <PASSWORD>
```

## How to use

To see how your app is used in flows, you might run `flow.py` which contains a simple flow with your app. To run it, just
execute the following command:

```bash

python flow.py
```
