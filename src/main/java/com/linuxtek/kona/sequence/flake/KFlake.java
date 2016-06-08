package com.linuxtek.kona.sequence.flake;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.linuxtek.kona.sequence.flake.uuid.UUIDGen;
import com.linuxtek.kona.encryption.KEncryptUtil;
import com.linuxtek.kona.util.KStringUtil;

public class KFlake {
	private static Logger logger = Logger.getLogger(KFlake.class);
    
	private static KFlake instance = new KFlake();

	// Get the MAC address (i.e., the "node" from a UUID1)
	private final long clockSeqAndNode = UUIDGen.getClockSeqAndNode();

	private final byte[] node = new byte[] { 
			(byte) ((clockSeqAndNode >> 40) & 0xff),
			(byte) ((clockSeqAndNode >> 32) & 0xff), 
			(byte) ((clockSeqAndNode >> 24) & 0xff),
			(byte) ((clockSeqAndNode >> 16) & 0xff), 
			(byte) ((clockSeqAndNode >> 8) & 0xff),
			(byte) ((clockSeqAndNode >> 0) & 0xff), 
	};
    
	private final ThreadLocal<ByteBuffer> tlbb = new ThreadLocal<ByteBuffer>() {
		@Override
		public ByteBuffer initialValue() {
			return ByteBuffer.allocate(16);
		}
	};

	private volatile int seq;
	private volatile long lastTimestamp;
	private final Object lock = new Object();

	private final int maxShort = (int) 0xffff;

	public byte[] getIdAsByte() {
		if (seq == maxShort) {
			throw new RuntimeException("Too fast");
		}

		long time;
		synchronized (lock) {
			time = System.currentTimeMillis();
			if (time != lastTimestamp) {
				lastTimestamp = time;
				seq = 0;
			}
			seq++;
			ByteBuffer bb = tlbb.get();
			bb.rewind();
			bb.putLong(time);
			bb.put(node);
			bb.putShort((short) seq);
			return bb.array();
		}
	}

	public String getIdAsString() {
		return getIdAsString(null);
	}

	public String getIdAsString(Integer charCount) {
		byte[] ba = getIdAsByte();
		ByteBuffer bb = ByteBuffer.wrap(ba);
		long ts = bb.getLong();
		int node_0 = bb.getInt();
		short node_1 = bb.getShort();
		short seq = bb.getShort();

		if (charCount == null) {
			return String.format("%016d-%s%s-%04d", ts, Integer.toHexString(node_0), Integer.toHexString(node_1), seq);
		}

		Long nextNo = ts + node_0 + node_1 + seq;
		return KStringUtil.toHex(nextNo, charCount);
	}

	private Long _getIdAsLong() {
		byte[] ba = getIdAsByte();
		ByteBuffer bb = ByteBuffer.wrap(ba);
		long ts = bb.getLong();
		int node_0 = bb.getInt();
		short node_1 = bb.getShort();
		short seq = bb.getShort();
		Long nextNo = ts + node_0 + node_1 + seq;
		return nextNo;
	}

	public static String getId(Integer charCount) {
		return instance.getIdAsString(charCount);
	}

	public static String getId() {
		return getId(null);
	}

	public static String getIdMD5() {
		String id = getId(null);
		try {
			id = KEncryptUtil.MD5(id).toUpperCase();
		} catch (Exception e) {
			logger.error(e);
		}
		return id;
	}

	public static Long getIdAsLong() {
		return instance._getIdAsLong();
	}

	public static void main(String[] args) throws IOException {
		int n = Integer.parseInt(args[0]);

		for (int i = 0; i < n; i++) {
			System.out.write(instance.getIdAsByte());
			// System.out.println(flake.getStringId());
		}
	}
}
