package net.pigsky.spring.amqp.message.converter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.AbstractMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;

/**
 * KryoMessageConverter is used to handle conversion between Java Object and bytes via Kryo
 * <p/>
 * Created by addo on 16/6/29.
 */
public class KryoMessageConverter extends AbstractMessageConverter {

    public static final String CONTENT_TYPE = "application/x-kryo";
    public static final String DEFAULT_CHARSET = "UTF-8";
    private String defaultCharset = DEFAULT_CHARSET;
    private KryoFactory kryoFactory = new DefaultKryoFactory();

    /**
     * Crate a message from the payload object and message properties provided. The message id will be added to the
     * properties if necessary later.
     *
     * @param object            the payload
     * @param messageProperties the message properties (headers)
     * @return a message
     */
    @Override
    protected Message createMessage(Object object, MessageProperties messageProperties) {
        byte[] bytes = null;
        Kryo kryo = kryoFactory.create();
        Output output = new ByteBufferOutput(4096, 1024 * 1024);
        try {
            kryo.writeClassAndObject(output, object);
            bytes = output.toBytes();
        } finally {
            output.close();
        }
        messageProperties.setContentType(CONTENT_TYPE);
        if (messageProperties.getContentEncoding() == null) {
            messageProperties.setContentEncoding(defaultCharset);
        }
        return new Message(bytes, messageProperties);
    }

    @Override
    public Object fromMessage(Message message) throws MessageConversionException {
        Object content = null;
        MessageProperties properties = message.getMessageProperties();
        if (properties != null) {
            if (properties.getContentType() != null && properties.getContentType().contains("x-kryo")) {
                Kryo kryo = kryoFactory.create();
                content = kryo.readClassAndObject(new ByteBufferInput(message.getBody()));
            } else {
                throw new MessageConversionException("Converter not applicable to this message");
            }
        }
        return content;
    }

    private class DefaultKryoFactory implements KryoFactory {

        @Override
        public Kryo create() {
            Kryo kryo = new Kryo();
            return kryo;
        }
    }
}
