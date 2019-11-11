/*
 * PeerMqDeliveryToken.java - An implementation of delivery token.
 * 
 * Copyright (c) 2016 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIQT package for more in detail.
 */
package org.piax.pubsub.stla;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.piax.ayame.tracer.jaeger.GlobalJaegerTracer;
import org.piax.ayame.tracer.message.TracerMessage;
import org.piax.ayame.tracer.message.TracerMessageBuilder;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Option.BooleanOption;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.Lower;
import org.piax.gtrans.RequestTransport.Response;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.ov.Overlay;
import org.piax.pubsub.MqActionListener;
import org.piax.pubsub.MqCallback;
import org.piax.pubsub.MqDeliveryToken;
import org.piax.pubsub.MqException;
import org.piax.pubsub.MqMessage;
import org.piax.pubsub.MqTopic;
import org.piax.pubsub.stla.Delegator.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.tracerresolver.TracerResolver;

public class PeerMqDeliveryToken implements MqDeliveryToken {
    private static final Logger logger = LoggerFactory
            .getLogger(PeerMqDeliveryToken.class);
    final MqMessage m;
    // Overlay<KeyRange<LATKey>, LATKey> o;
    final Overlay<Destination, LATKey> o;

    boolean isComplete = false;
    MqActionListener aListener = null;
    Object userContext = null;
    MqCallback c = null;
    int seqNo = 0;
    public static int ACK_INTERVAL = -1;
    public static BooleanOption USE_DELEGATE = new BooleanOption(true, "-use-delegate");
    ConcurrentHashMap<String, DeliveryDelegator> delegators;
    CompletableFuture<Void> completionFuture;
    HashMap<String, CompletableFuture<Boolean>> publisherKeyFuture;
    UUID spanID;

    public PeerMqDeliveryToken(Overlay<Destination, LATKey> overlay,
                               MqMessage message, MqCallback callback, int seqNo) {

        assert message != null;
        this.m = message;
        this.o = overlay;
        this.c = callback;
        this.seqNo = seqNo;

        try {
            MqTopic mqTopic = new MqTopic(message.getTopic());
            List<String> pubKeys = mqTopic.getPublisherKeys();
            publisherKeyFuture = new HashMap<>();
            pubKeys.forEach(publisherKey -> publisherKeyFuture.put(publisherKey, new CompletableFuture<>()));
        } catch (MqException e) {
            e.printStackTrace();
        }

        if (Objects.nonNull(publisherKeyFuture)) {
            CompletableFuture[] booleanCompletableFutures =
                    publisherKeyFuture.values().toArray(new CompletableFuture[0]);
            completionFuture = CompletableFuture.allOf(booleanCompletableFutures);
        } else {
            completionFuture = new CompletableFuture<>();
        }
        delegators = new ConcurrentHashMap<>();
    }

    public void startDeliveryWithDelegators(PeerMqEngine engine,
                                            String[] kStrings) throws MqException {
        for (int i = 0; i < kStrings.length; i++) {
            NearestDelegator nd = engine.getNearestDelegator(kStrings[i]);
            if (nd != null && nd.getEndpoint() != null) { // XXX ...and not expired.
                DeliveryDelegator d = new DeliveryDelegator(nd);
                delegators.put(kStrings[i], d);
                logger.debug("found existing delegator for {} : {}", kStrings[i], nd);
                String kStr = kStrings[i];
                // deliver to the kString area via d.endpoint.
                CompletableFuture<Void> cf = engine.delegate(this, d.endpoint, kStr, m);
                cf.whenComplete((res, ex) -> {
                    if (ex != null) { // some error occured.
                        logger.info("An error occured on delegator" + ex.getMessage());
                        engine.removeDelegator(kStr);
                    }
                });
                //engine.delegate(this, engine.getEndpoint(), kStrings[i], m);
            } else {
                DeliveryDelegator d = new DeliveryDelegator(kStrings[i]);
                delegators.put(kStrings[i], d);
                findDelegatorAndDeliver(d, engine, kStrings[i]);
            }
        }
    }

    /*
     *
     */
    public DeliveryDelegator findDelegatorAndDeliver(DeliveryDelegator d, PeerMqEngine engine,
                                                     String kString) throws MqException {
        try {
            LATopic lat = new LATopic(kString);
            if (engine.getClusterId() == null) {
                lat = LATopic.clusterMax(lat);
            } else {
                lat.setClusterId(engine.getClusterId()); // min of the cluster id
            }
            logger.debug("finding delegator for '{}'", kString);
            o.requestAsync(new Lower<LATKey>(false, new LATKey(lat), 1),
                           new ControlMessage(engine.getEndpoint(), seqNo, kString, m),
                           (ep, ex) -> {
                               if (Response.EOR.equals(ep)) {
                                   logger.debug("EOR. delegator for '{}' finished", kString);
                               } else if (ex == null) {
                                   // the result can be null
                                   d.setEndpoint((Endpoint) ep);
                                   logger.debug("delegator for '{}' is {}", kString, ep);
                                   if (ep == null) { // finish because not found.
                                       d.setSucceeded();
                                       delegationFinished();
                                   } else {
                                       // cache the delegator if found.
                                       engine.foundDelegator(kString, d);
                                       // not finish because delivery is going on.
                                   }
                               } else {
                                   // some exception occured while find & deliver.
                                   // note that the d.endpoint is null in this case.
                                   d.setFailured(ex);
                               }
                           },
                           new TransOptions(ResponseType.DIRECT,
                                            m.getQos() == 0 ? RetransMode.NONE
                                                            : RetransMode.FAST));
        } catch (Exception e) {
            throw new MqException(e);
        }
        return d;
    }

    boolean isAllDelegationCompleted() {
        for (DeliveryDelegator d : delegators.values()) {
            if (!d.isFinished()) {
                logger.debug("delegationCompleted: not finished: '{}'", d.getKeyString());
                return false;
            }
        }
        logger.debug("delegationCompleted: completed {}", m.getTopic());
        return true;
    }

    public void replaceExistingDeliveryDelegator(DeliveryDelegator repl) {
        delegators.put(repl.getKeyString(), repl);
    }

    public void delegationSucceeded(String kString) {
        DeliveryDelegator d = delegators.get(kString);
        if (d != null) {
            d.setSucceeded();
        } else {
            logger.debug("delegationSucceeded: not found {}", kString);
        }
        delegationFinished();
    }

    private boolean delegationFinished() {
        if (!isComplete && isAllDelegationCompleted()) {
            if (aListener != null) {
                aListener.onSuccess(this);
            }
            if (c != null) {
                c.deliveryComplete(this);
            }
            logger.debug("finished delivery: {}", m);
            isComplete = true;
            return true;
        }
        return false;
    }

    public void startDelivery(PeerMqEngine engine) throws MqException {
        if (false) {
            startDeliveryDelegate(engine);
        } else {
            startDeliveryEach(engine);
        }
    }

    public void startDeliveryDelegate(PeerMqEngine engine) throws MqException {
        String[] kStrings = new MqTopic(m.getTopic()).getPublisherKeyStrings();
        startDeliveryWithDelegators(engine, kStrings);
        logger.debug("delegators={}", delegators);
    }

    public void startDeliveryEach(PeerMqEngine engine) throws MqException {
        Tracer tracer = TracerResolver.resolveTracer();
        String topic = m.getTopic();
        MqTopic mqTopic = new MqTopic(topic);
        String[] pStrs = mqTopic.getPublisherKeyStrings();
        final Span parentSpan = tracer.activeSpan();
        if (logger.isInfoEnabled()) {
            logger.info("all of MqTopic {}", mqTopic);
            logger.info(String.format("async point parentSpan=%s", parentSpan.context().toString()));
        }
        for (String pStr : pStrs) {
            final Span childSpan = tracer.buildSpan("startDeliveryEach").asChildOf(parentSpan).start();
            String childSpanLogMsg = String.format("async point: parentSpan=%s, child=%s",
                                                   parentSpan.context().toString(),
                                                   childSpan.context().toString());
            logger.info(childSpanLogMsg);
            try (Scope ignored = tracer.activateSpan(childSpan)) {
                startDeliveryForPublisherKeyString(engine, pStr);
            }
        }
    }

    public void startDeliveryForPublisherKeyString(PeerMqEngine engine, String kString) throws MqException {
        Tracer tracer = TracerResolver.resolveTracer();
        final Span span = tracer.activeSpan();
        span.log(TracerMessageBuilder.fastBuild(logger, "(topic=" + kString + ")", this));
        try {
            RetransMode mode;
            ResponseType type;
            TransOptions opts;
            switch (m.getQos()) {
                case 0:
                    type = ResponseType.NO_RESPONSE;
                    if (ACK_INTERVAL < 0) {
                        mode = RetransMode.NONE;
                    } else {
                        mode = (seqNo % ACK_INTERVAL == 0) ? RetransMode.NONE_ACK
                                                           : RetransMode.NONE;
                    }
                    opts = new TransOptions(type, mode);
                    break;
                default: // 1, 2
                    type = ResponseType.AGGREGATE;
                    mode = RetransMode.FAST;
                    opts = new TransOptions(PeerMqEngine.DELIVERY_TIMEOUT, type,
                                            mode);
                    break;
            }
            o.requestAsync(
                    new KeyRange<LATKey>(new LATKey(LATopic.topicMin(kString))
                            ,new LATKey(LATopic.topicMax(kString))), (Object) m
                    , (res, ex) -> {
                        GlobalJaegerTracer.scheduledFinishing(span);
                        if (res == Response.EOR) {
                            if (aListener != null) {
                                aListener.onSuccess(this);
                            }
                            logger.info("cf({}, key{})", completionFuture.toString(), kString);
                            publisherKeyFuture.get(kString).complete(true);
                            //completionFuture.complete(true);
                            if (c != null) {
                                c.deliveryComplete(this);
                            }
                            isComplete = true;
                        }
                        // res is the
                        if (ex != null) {
                            completionFuture.completeExceptionally(ex);
                            if (aListener != null) {
                                aListener.onFailure(this, ex);
                            }
                        }
                    },
                    opts);
        } catch (Exception e) {
            if (aListener != null) {
                aListener.onFailure(this, e);
            }
            throw new MqException(e);
        }
    }

    @Override
    @Deprecated
    public void waitForCompletion() throws MqException {
        Tracer tracer = TracerResolver.resolveTracer();
        Span span = tracer.activeSpan();
        try {
            // NOTE: 1-msg = 1-CompFuture, all-masgs = completionFuture
            completionFuture.thenRunAsync(() -> {
                TracerMessage<String, String> tracerMessage = TracerMessageBuilder
                        .fastBuild(logger, "MqDeliveryToken get completionFuture", this);
                span.log(tracerMessage);
                GlobalJaegerTracer.scheduledFinishing(span);
            });
        } catch (Exception e) {
            if (aListener != null) {
                aListener.onFailure(this, e);
            }
            throw new MqException(e);
        }
    }

    @Override
    @Deprecated
    public void waitForCompletion(long timeout) throws MqException {
        try {
            completionFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (aListener != null) {
                aListener.onFailure(this, e);
            }
            throw new MqException(e);
        }
    }

    @Override
    public boolean isComplete() {
        return isComplete;
    }

    @Override
    public MqException getException() {
        return null;
    }

    @Override
    public void setActionCallback(MqActionListener listener) {
        aListener = listener;
    }

    @Override
    public MqActionListener getActionCallback() {
        return aListener;
    }

    @Override
    public String[] getTopics() {
        return new String[] { m.getTopic() };
    }

    @Override
    public void setUserContext(Object userContext) {
        this.userContext = userContext;
    }

    @Override
    public Object getUserContext() {
        return this.userContext;
    }

    @Override
    public int getMessageId() {
        // XXX Message id has no meaning
        return 0;
    }

    @Override
    public MqMessage getMessage() {
        return m;
    }
    
    @Override
    public String toString() {
        return "PeerMqDeliveryToken{" +
               "m=" + m +
               ", o=" + o +
               ", isComplete=" + isComplete +
               ", aListener=" + aListener +
               ", userContext=" + userContext +
               ", c=" + c +
               ", seqNo=" + seqNo +
               ", delegators=" + delegators +
               ", completionFuture=" + completionFuture +
               ", publisherKeyFuture=" + publisherKeyFuture +
               ", spanID=" + spanID +
               '}';
    }
}