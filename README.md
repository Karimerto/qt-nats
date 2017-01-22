# qt-nats
A [Qt5](https://www.qt.io) C++11 client for the [NATS messaging system](https://nats.io).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Language (C++)](https://img.shields.io/badge/powered_by-C++-green.svg?style=flat-square)](https://img.shields.io/badge/powered_by-C++-green.svg?style=flat-square)

## Installation

This is a header-only library that depends on Qt5. All you have to do is include it inside your
project:

```
#include <QCoreApplication>
#include "natsclient.h"

int main(int argc, char *argv[])
{
    QCoreApplication a(argc, argv);

    Nats::Client client;
    client.connect("127.0.0.1", 4222, [&]
    {
        // simple subscribe
        client.subscribe("foo", [](auto message, auto inbox, auto subject)
        {
            qDebug() << "received: " << message << inbox << subject;
        });

        // simple publish
        client.publish("foo", "Hello NATS!");
    });

    return a.exec();
}
```

## Basic usage

```
Nats::Client client;
client.connect("127.0.0.1", 4222, [&]
{
    // simple publish
    client.publish("foo", "Hello World!");

    // simple subscribe
    client.subscribe("foo", [](auto message, auto /* inbox */, auto /* subject */)
    {
        qDebug() << "received message: " << message;
    });

    // unsubscribe
    int sid = client.subscribe("foo", [](auto, auto, auto){});
    client.unsubscribe(sid);

    // request
    client.request("help", [&](auto /* message */, auto reply_inbox, auto /* subject */)
    {
        client.publish(reply_inbox, "I can help");
    });
});
````

## Authentication


```
Nats::Client client;
Nats::Options options;

options.user = "user";
options.pass = "pass";

client.connect("127.0.0.1", 4222, options, []
{
    ...
});
```
