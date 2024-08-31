#ifndef NATSCLIENT_H
#define NATSCLIENT_H

#include <QHash>
#include <QJsonDocument>
#include <QJsonObject>
#include <QMultiMap>
#include <QObject>
#include <QProcessEnvironment>
#include <QSslConfiguration>
#include <QSslSocket>
#include <QStringBuilder>
#include <QTextStream>
#include <QUuid>

namespace Nats
{
    #define DEBUG(x) do { if (_debug_mode) { qDebug() << x; } } while (0)

    //! Header type
    typedef QMultiMap<QString, QString> Headers;

    //! main callback message
    using MessageCallback = std::function<void(QByteArray &&message, QString &&reply, QString &&subject, Headers &&headers)>;
    using ConnectCallback = std::function<void()>;

    //!
    //! \brief CRLF
    //! NATS protocol separator
    const QByteArray CRLF = "\r\n";

    //!
    //! \brief hdrLine
    //! NATS headers indicator
    const QByteArray hdrLine = "NATS/1.0\r\n";

    const int hdrPreEnd = hdrLine.length() - CRLF.length();
    const QString statusHdr("Status");
    const QString descrHdr("Description");
    const QString noResponders("503");
    const QString noMessagesSts("404");
    const QString reqTimeoutSts("408");
    const QString jetStream409Sts("409");
    const QString controlMsg("100");
    const int statusLen = 3;

    //! convert QMultiMap headers into a suitable QByteArray
    static const QByteArray makeHeader(const Headers &headers)
    {
        if(headers.size() == 0)
        {
            return QByteArray();
        }

        // Start with the header line
        QByteArray result(hdrLine);

        // Append values
        QMapIterator<QString, QString> i(headers);
        while(i.hasNext())
        {
            i.next();
            // It might be a good idea to check the headers are valid, but
            // for now, leave it up to the user
            result += i.key().toUtf8() + ": " + i.value().toUtf8() + CRLF;
        }

        // Add a final CRLF
        result += CRLF;

        return result;
    }

    //! convert QByteArray headers back into a QMultiMap
    static const Headers parseHeader(const QByteArray &hdr)
    {
        QTextStream str(hdr);
        QString firstline = str.readLine();
        if(firstline.length() < hdrPreEnd || hdrLine.left(hdrPreEnd) != firstline.left(hdrPreEnd))
        {
            qCritical() <<  "invalid header" << firstline;
            return Headers();
        }

        Headers headers;
        QString line;
        while(str.readLineInto(&line))
        {
            int idx = line.indexOf(':');
            // If there is no colon, skip this line
            if(idx < 1)
            {
                continue;
            }

            // Parse key and value
            QString key(line.left(idx));
            idx++;
            QString value(line.mid(idx).trimmed());

            // Skip empty values as well
            if(value.isEmpty())
            {
                continue;
            }
            headers.insert(key, value);
        }

        // Parse inline status, if any
        if(firstline.length() > hdrPreEnd)
        {
            QString description;
            QString status = firstline.mid(hdrPreEnd).trimmed();
            if(status.length() != statusLen)
            {
                description = status.mid(statusLen).trimmed();
                status = status.left(statusLen);
            }
            headers.insert(statusHdr, status);
            if(!description.isEmpty())
            {
                headers.insert(descrHdr, description);
            }
        }

        return headers;
    }

    //!
    //! \brief The Options struct
    //! holds all client options
    struct Options
    {
        bool verbose = false;
        bool pedantic = false;
        bool headers = true;
        bool tls_required = false;
        bool ssl = false;
        bool ssl_verify = true;
        QString ssl_key;
        QString ssl_cert;
        QString ssl_ca;
        QString name = "qt-nats";
        const QString lang = "cpp";
        const QString version = "1.0.0";
        QString user;
        QString pass;
        QString token;
    };

    class Client;

    //!
    //! \brief The Subscription class
    //! holds subscription data and emits signal when ready as alternative to callbacks
    class Subscription : public QObject
    {
        Q_OBJECT

    public:
        explicit Subscription(Client* client, QObject *parent = nullptr): QObject(parent), m_client(client) {}

        QString subject;
        QByteArray message;
        QString reply;
        Headers headers;
        uint64_t ssid = 0;

    signals:
        void received();

    public slots:
        void unsubscribe(int max_messages = 0);

    private:
        Client *m_client;
    };

    //!
    //! \brief The Client class
    //! main client class
    class Client : public QObject
    {
        Q_OBJECT
    public:
        explicit Client(QObject *parent = nullptr);

        //!
        //! \brief publish
        //! \param subject
        //! \param message
        //! \param headers
        //! publish given message with subject, optionally with headers
        void publish(const QString &subject, const QByteArray &message, const QString &reply);
        void publish(const QString &subject, const QByteArray &message = "");
        void publish(const QString &subject, const Headers &headers, const QByteArray &message = "", const QString &reply = "");

        //!
        //! \brief subscribe
        //! \param subject
        //! \param callback
        //! \return subscription id
        //! subscribe to given subject
        //! when message is received, callback is fired
        uint64_t subscribe(const QString &subject, Nats::MessageCallback callback);

        //!
        //! \brief subscribe
        //! \param subject
        //! \param queue
        //! \param callback
        //! \return subscription id
        //! overloaded function
        //! subscribe to given subject and queue
        //! when message is received, callback is fired
        //! each message will be delivered to only one subscriber per queue group
        uint64_t subscribe(const QString &subject, const QString &queue, Nats::MessageCallback callback);

        //!
        //! \brief subscribe
        //! \param subject
        //! \return
        //! return subscription class holding result for signal/slot version
        Subscription *subscribe(const QString &subject);

        //!
        //! \brief subscribe
        //! \param subject
        //! \param queue
        //! \return
        //! return subscription class holding result for signal/slot version
        Subscription *subscribe(const QString &subject, const QString &queue);

        //!
        //! \brief unsubscribe
        //! \param ssid
        //! \param max_messages
        //!
        void unsubscribe(uint64_t ssid, int max_messages = 0);

        //!
        //! \brief request
        //! \param subject
        //! \param message
        //! \return
        //! make request using given subject and optional message
        uint64_t request(const QString subject, const QByteArray message, Nats::MessageCallback callback);
        uint64_t request(const QString subject, Nats::MessageCallback callback);
        uint64_t request(const QString subject, const QByteArray message, const Headers headers, Nats::MessageCallback callback);
        uint64_t request(const QString subject, const Headers headers, Nats::MessageCallback callback);

    signals:

        //!
        //! \brief connected
        //! signal that the client is connected
        void connected();

        //!
        //! \brief error
        //! signal when there is an error connecting
        void error(const QString);

        //!
        //! \brief disconnected
        //! signal that the client is disconnected
        void disconnected();

    public slots:

        //!
        //! \brief connect
        //! \param host
        //! \param port
        //! connect to server with given host and port options
        //! after valid connection is established 'connected' signal is emmited
        void connect(const QString &host = "127.0.0.1", quint16 port = 4222, Nats::ConnectCallback callback = nullptr);
        void connect(const QString &host, quint16 port, const Nats::Options &options, Nats::ConnectCallback callback = nullptr);

        //!
        //! \brief disconnect
        //! disconnect from server by closing socket
        void disconnect();

        //!
        //! \brief connectSync
        //! \param host
        //! \param port
        //! synchronous version of connect, waits until connections with nats server is valid
        //! this will still fire 'connected' signal if one wants to use that instead
        bool connectSync(const QString &host = "127.0.0.1", quint16 port = 4222);
        bool connectSync(const QString &host, quint16 port, const Nats::Options &options);

    private:

        //!
        //! \brief debug_mode
        //! extra debug output, can be set with enviroment variable DEBUG=qt-nats
        bool _debug_mode = false;

        //!
        //! \brief m_buffer
        //! main buffer that holds data from server
        QByteArray m_buffer;

        //!
        //! \brief m_ssid
        //! subscribtion id holder
        uint64_t m_ssid = 0;

        //!
        //! \brief m_socket
        //! main tcp socket
        QSslSocket m_socket;

        //!
        //! \brief m_options
        //! client options
        Options m_options;

        //!
        //! \brief m_callbacks
        //! subscription callbacks
        QHash<uint64_t, MessageCallback> m_callbacks;

        //!
        //! \brief m_info
        //! latest received info message
        QJsonObject m_info;

        //!
        //! \brief send_info
        //! \param options
        //! send client information and options to server
        void send_info(const Options &options);

        //!
        //! \brief parse_info
        //! \param message
        //! \return
        //! parse INFO message from server and return json object with data
        QJsonObject parse_info(const QByteArray &message);

        //!
        //! \brief set_listeners
        //! set connection listeners
        void set_listeners();

        //!
        //! \brief process_inbound
        //! \param buffer
        //! process messages from buffer
        bool process_inbound(const QByteArray &buffer);
    };

    inline Client::Client(QObject *parent) : QObject(parent)
    {
        QProcessEnvironment env = QProcessEnvironment::systemEnvironment();
        _debug_mode = (env.value(QStringLiteral("DEBUG")).indexOf("qt-nats") != -1);

        if(_debug_mode)
            DEBUG("debug mode");
    }

    inline void Client::connect(const QString &host, quint16 port, ConnectCallback callback)
    {
        connect(host, port, m_options, callback);
    }

    inline void Client::connect(const QString &host, quint16 port, const Options &options, ConnectCallback callback)
    {
        // Check is client socket is already connected and return if it is
        if (m_socket.isOpen())
            return;

        QObject::connect(&m_socket, &QAbstractSocket::errorOccurred, this, [this](QAbstractSocket::SocketError socketError)
        {
            DEBUG(socketError);

            emit error(m_socket.errorString());
        });

        QObject::connect(&m_socket, static_cast<void(QSslSocket::*)(const QList<QSslError> &)>(&QSslSocket::sslErrors), this, [this](const QList<QSslError> &errors)
        {
            DEBUG(errors);

            emit error(m_socket.errorString());
        });

        QObject::connect(&m_socket, &QSslSocket::encrypted, this, [this, options, callback]
        {
            DEBUG("SSL/TLS successful");

            send_info(options);
            set_listeners();

            if(callback)
                callback();

            emit connected();
        });

        QObject::connect(&m_socket, &QSslSocket::disconnected, this, [this]()
        {
            DEBUG("socket disconnected");
            emit disconnected();

            // Disconnect everything connected to an m_socket's signals
            QObject::disconnect(&m_socket, nullptr, nullptr, nullptr);
        });

        // receive first info message and disconnect
        auto signal = std::make_shared<QMetaObject::Connection>();
        *signal = QObject::connect(&m_socket, &QSslSocket::readyRead, this, [this, signal, options, callback]
        {
            QObject::disconnect(*signal);
            QByteArray info_message = m_socket.readAll();

            QJsonObject json = parse_info(info_message);
            bool tls_required = json.value(QStringLiteral("tls_required")).toBool();

            // if client or server wants ssl start encryption
            if(options.ssl || options.tls_required || tls_required)
            {
                DEBUG("starting SSL/TLS encryption");

                if(!options.ssl_verify)
                    m_socket.setPeerVerifyMode(QSslSocket::VerifyNone);

                if(!options.ssl_ca.isEmpty())
                {
                    QSslConfiguration config = m_socket.sslConfiguration();
                    config.setCaCertificates(QSslCertificate::fromPath(options.ssl_ca));
                }

                if(!options.ssl_key.isEmpty())
                    m_socket.setPrivateKey(options.ssl_key);

                if(!options.ssl_cert.isEmpty())
                    m_socket.setLocalCertificate(options.ssl_cert);

                m_socket.startClientEncryption();
            }
            else
            {
                send_info(options);
                set_listeners();

                if(callback)
                    callback();

                emit connected();
            }
        });


        DEBUG("connect started" << host << port);

        m_socket.connectToHost(host, port);
    }

    inline void Client::disconnect()
    {
        m_socket.flush();
        m_socket.close();
    }

    inline bool Client::connectSync(const QString &host, quint16 port)
    {
        return connectSync(host, port, m_options);
    }

    inline bool Client::connectSync(const QString &host, quint16 port, const Options &options)
    {
         QObject::connect(&m_socket, &QAbstractSocket::errorOccurred, this, [this](QAbstractSocket::SocketError socketError)

        {
            DEBUG(socketError);

            emit error(m_socket.errorString());
        });

        m_socket.connectToHost(host, port);
        if(!m_socket.waitForConnected())
            return false;

        if(!m_socket.waitForReadyRead())
            return false;

        // receive first info message
        auto info_message = m_socket.readAll();

        QJsonObject json = parse_info(info_message);
        bool tls_required = json.value(QStringLiteral("tls_required")).toBool();

        // if client or server wants ssl start encryption
        if(options.ssl || options.tls_required || tls_required)
        {
            DEBUG("starting SSL/TLS encryption");

            if(!options.ssl_verify)
                m_socket.setPeerVerifyMode(QSslSocket::VerifyNone);

            if(!options.ssl_ca.isEmpty())
            {
                QSslConfiguration config = m_socket.sslConfiguration();
                config.setCaCertificates(QSslCertificate::fromPath(options.ssl_ca));
            }

            if(!options.ssl_key.isEmpty())
                m_socket.setPrivateKey(options.ssl_key);

            if(!options.ssl_cert.isEmpty())
                m_socket.setLocalCertificate(options.ssl_cert);

            m_socket.startClientEncryption();

            if(!m_socket.waitForEncrypted())
                return false;
        }

        send_info(options);
        set_listeners();

        emit connected();

        return true;
    }

    inline void Client::send_info(const Options &options)
    {
        QJsonObject info{
            {"verbose", options.verbose},
            {"pedantic", options.pedantic},
            {"headers", options.headers},
            {"tls_required", options.tls_required},
            {"name", options.name},
            {"lang", options.lang},
            {"version", options.version},
            {"protocol", 1},
        };

        // Set username, if any
        if (!options.user.isEmpty())
        {
            info.insert("user", options.user);
        }

        // Set password, if any
        if (!options.user.isEmpty())
        {
            info.insert("user", options.user);
        }

        // Set authorization token, if any
        if (!options.user.isEmpty())
        {
            info.insert("auth_token", options.token);
        }

        // If headers are enabled, also set no_responders
        if (options.headers)
        {
            info.insert("no_responders", true);
        }

        // Compose CONNECT message
        QByteArray message("CONNECT ");
        message += QJsonDocument(info).toJson(QJsonDocument::Compact);
        message += CRLF;

        DEBUG("send info message:" << message);

        m_socket.write(message);
    }

    inline QJsonObject Client::parse_info(const QByteArray &message)
    {
        DEBUG(message);

        // discard 'INFO '
        m_info = QJsonDocument::fromJson(message.mid(5)).object();
        return m_info;
    }

    inline void Client::publish(const QString &subject, const QByteArray &message)
    {
        publish(subject, message, "");
    }

    inline void Client::publish(const QString &subject, const QByteArray &message, const QString &reply)
    {
        QString body = QStringLiteral("PUB ") % subject % " " % reply % (reply.isEmpty() ? "" : " ") % QString::number(message.length()) % CRLF % message % CRLF;

        DEBUG("published:" << body);

        m_socket.write(body.toUtf8());
    }

    inline void Client::publish(const QString &subject, const Headers &headers, const QByteArray &message, const QString &reply)
    {
        // Send either PUB or HPUB, depending on header(s)
        QString body;

        // Convert headers to string
        QByteArray hdr = makeHeader(headers);
        if (hdr.length() > 0)
        {
            body = QStringLiteral("HPUB ") % subject % " " % \
            reply % (reply.isEmpty() ? "" : " ") % \
            QString::number(hdr.length()) % " " % \
            QString::number(hdr.length() + message.length()) % CRLF % hdr;
        }
        else
        {
            body = QStringLiteral("PUB ") % subject % " " % \
            reply % (reply.isEmpty() ? "" : " ") % \
            QString::number(message.length()) % CRLF;
        }

        // Append message
        body += message % CRLF;

        DEBUG("published:" << body);

        m_socket.write(body.toUtf8());
    }

    inline uint64_t Client::subscribe(const QString &subject, MessageCallback callback)
    {
        return subscribe(subject, "", callback);
    }

    inline uint64_t Client::subscribe(const QString &subject, const QString &queue, MessageCallback callback)
    {
        m_callbacks[++m_ssid] = callback;

        QString message = QStringLiteral("SUB ") % subject % " " % queue % (queue.isEmpty() ? "" : " ") % QString::number(m_ssid) % CRLF;

        m_socket.write(message.toUtf8());

        DEBUG("subscribed:" << message);

        return m_ssid;
    }

    inline Subscription *Client::subscribe(const QString &subject)
    {
        return subscribe(subject, "");
    }

    inline Subscription *Client::subscribe(const QString &subject, const QString &queue)
    {
        auto subscription = new Subscription(this);

        subscription->ssid = subscribe(subject, queue, [subscription](const QByteArray &message, const QString &reply, const QString &subject, const Headers &headers)
        {
            subscription->message = message;
            subscription->subject = subject;
            subscription->reply = reply;
            subscription->headers = headers;

            emit subscription->received();
        });

        return subscription;
    }

    inline void Client::unsubscribe(uint64_t ssid, int max_messages)
    {
        QString message = QStringLiteral("UNSUB ") % QString::number(ssid) % (max_messages > 0 ? QString(" %1").arg(max_messages) : "") % CRLF;

        DEBUG("unsubscribed:" << message);

        m_socket.write(message.toUtf8());
    }

    inline uint64_t Client::request(const QString subject, MessageCallback callback)
    {
        return request(subject, "", callback);
    }

    inline uint64_t Client::request(const QString subject, const QByteArray message, MessageCallback callback)
    {
        QString reply = QUuid::createUuid().toString();
        uint64_t ssid = subscribe(reply, callback);
        unsubscribe(ssid, 1);
        publish(subject, message, reply);

        return ssid;
    }

    inline uint64_t Client::request(const QString subject, const Headers headers, Nats::MessageCallback callback)
    {
        return request(subject, "", headers, callback);
    }

    inline uint64_t Client::request(const QString subject, const QByteArray message, const Headers headers, Nats::MessageCallback callback)
    {
        QString reply = QUuid::createUuid().toString();
        uint64_t ssid = subscribe(reply, callback);
        unsubscribe(ssid, 1);
        publish(subject, headers, message, reply);

        return ssid;
    }

    //! TODO: disconnect handling
    inline void Client::set_listeners()
    {
        DEBUG("set listeners");
        QObject::connect(&m_socket, &QSslSocket::readyRead, this, [this]
        {
            // add new data to buffer
            m_buffer +=  m_socket.readAll();

            // process message if exists
            int CRLF_pos = m_buffer.lastIndexOf(CRLF);
            if(CRLF_pos != -1)
            {
                QByteArray msg_buffer = m_buffer.left(CRLF_pos + CRLF.length());
                process_inbound(msg_buffer);
            }
        });
    }

    // parse incoming messages, see http://nats.io/documentation/internals/nats-protocol
    // QStringView is used so we don't do unnecessary allocations
    // TODO: error on invalid message
    inline bool Client::process_inbound(const QByteArray &buffer)
    {
        DEBUG("handle message:" << buffer);

        // track movement inside buffer for parsing
        int last_pos = 0, current_pos = 0;
        bool is_hmsg;

        while(last_pos != buffer.length())
        {
            // we always get delimited message
            current_pos = buffer.indexOf(CRLF, last_pos);
            if(current_pos == -1)
            {
                qCritical() << "CRLF not found, this should not happen";
                break;
            }

            QString operation(buffer.mid(last_pos, current_pos - last_pos));

            // if this is PING operation, reply
            if(operation.compare(QStringLiteral("PING"), Qt::CaseInsensitive) == 0)
            {
                DEBUG("sending pong");
                m_socket.write(QString("PONG" % CRLF).toUtf8());
                last_pos = current_pos + CRLF.length();
                continue;
            }
            // +OK operation
            else if(operation.compare(QStringLiteral("+OK"), Qt::CaseInsensitive) == 0)
            {
                DEBUG("+OK");
                last_pos = current_pos + CRLF.length();
                continue;
            }
            // if -ERR, close client connection | -ERR <error message>
            else if(operation.indexOf("-ERR", 0, Qt::CaseInsensitive) != -1)
            {
                QStringView error_message = QStringView{operation}.mid(4);

                qCritical() << "error" << error_message;

                if(error_message.compare(QStringLiteral("Invalid Subject")) != 0)
                    m_socket.close();

                return false;
            }
            // MSG operation
            else if(operation.indexOf(QStringLiteral("MSG"), Qt::CaseInsensitive) == 0)
            {
                is_hmsg = false;
            }
            // HMSG operation
            else if(operation.indexOf(QStringLiteral("HMSG"), Qt::CaseInsensitive) == 0)
            {
                is_hmsg = true;
            }
            else
            {
                qCritical() << "invalid message - no message left";

                m_buffer.remove(0,current_pos + CRLF.length());
                return false;
            }

            // extract MSG or HMSG data
            // MSG format is: 'MSG <subject> <sid> [reply-to] <#bytes>\r\n[payload]\r\n'
            // HMSG format is: 'HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>\r\n[headers]\r\n\r\n[payload]\r\n'

            // extract total_len = bytes and check if there is a message in this buffer
            // if not, wait for next call, otherwise, extract all data

            int total_len = 0;
            int message_len = 0;
            int header_len = 0;
            QString subject, sid, reply;

            QList<QStringView> parts = QStringView{operation}.split(u" ", Qt::SkipEmptyParts);

            current_pos += CRLF.length();

            if(is_hmsg)
            {
                if(parts.length() == 5)
                {
                    header_len = parts[3].toInt();
                    total_len = parts[4].toInt();
                }
                else if (parts.length() == 6)
                {
                    reply = (parts[3]).toString();
                    header_len = parts[4].toInt();
                    total_len = parts[5].toInt();
                }
                else
                {
                    qCritical() <<  "invalid message - wrong length" << parts.length();
                    break;
                }
                message_len = total_len - header_len;
            }
            else
            {
                if(parts.length() == 4)
                {
                    total_len = message_len = parts[3].toInt();
                }
                else if (parts.length() == 5)
                {
                    reply = (parts[3]).toString();
                    total_len = message_len = parts[4].toInt();
                }
                else
                {
                    qCritical() <<  "invalid message - wrong length" << parts.length();
                    break;
                }
            }

            if(current_pos + total_len + CRLF.length() > buffer.length())
            {
                DEBUG("message not in buffer, waiting");
                break;
            }

            operation = parts[0].toString();
            subject = parts[1].toString();
            sid = parts[2].toString();
            uint64_t ssid = sid.toULong();
            QByteArray message;
            Headers headers;

            // Extract headers, if any
            QByteArray hdr;
            if(is_hmsg)
            {
                hdr = buffer.mid(current_pos, header_len);
                message = buffer.mid(current_pos + header_len, message_len);
                headers = parseHeader(hdr);
            }
            else
            {
                message = buffer.mid(current_pos, message_len);
            }

            // Update last position
            last_pos = current_pos + total_len + CRLF.length();

            DEBUG("message:" << message);

            // call correct subscription callback
            if(m_callbacks.contains(ssid))
            {
                auto callback = m_callbacks[ssid];
                callback(std::move(message), std::move(reply), std::move(subject), std::move(headers));
            }
            else
            {
                qWarning() << "invalid callback";
            }
        }

        // remove processed messages from buffer
        m_buffer.remove(0, last_pos);

        return true;
    }

    inline void Subscription::unsubscribe(int max_messages)
    {
        m_client->unsubscribe(ssid, max_messages);
    }

}

#endif // NATSCLIENT_H
