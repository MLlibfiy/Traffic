package com.shujia.spark.skynet;

import java.io.Serializable;

import scala.math.Ordered;

public class SpeedSortKey implements Ordered<SpeedSortKey>,Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long lowSpeed;
	private long normalSpeed;
	private long mediumSpeed;
	private long highSpeed;

	public SpeedSortKey() {
		super();
	}

	public SpeedSortKey(long lowSpeed, long normalSpeed, long mediumSpeed, long highSpeed) {
		super();
		this.lowSpeed = lowSpeed;
		this.normalSpeed = normalSpeed;
		this.mediumSpeed = mediumSpeed;
		this.highSpeed = highSpeed;
	}

	/**
	 * 大于
	 */
	@Override
	public boolean $greater(SpeedSortKey other) {
		if(highSpeed > other.getHighSpeed()) {
			return true;
		} else if(highSpeed == other.getHighSpeed() && 	mediumSpeed > other.getMediumSpeed()) {
			return true;
		} else if(highSpeed == other.getHighSpeed() &&
				mediumSpeed == other.getMediumSpeed() &&
				normalSpeed > other.getNormalSpeed()) {
			return true;
		} else if(highSpeed == other.getHighSpeed() &&
				mediumSpeed == other.getMediumSpeed() &&
				normalSpeed == other.getNormalSpeed() &&
				lowSpeed > other.getLowSpeed()) {
			return true;
		}
		return false;
	}

	/**
	 * 大于等于
	 */
	@Override
	public boolean $greater$eq(SpeedSortKey other) {
		if($greater(other)) {
			return true;
		}else if(highSpeed == other.getHighSpeed() && mediumSpeed == other.getMediumSpeed() && normalSpeed == other.getNormalSpeed() &&	lowSpeed == other.getLowSpeed()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(SpeedSortKey other) {
		return !$greater$eq(other);
	}

	@Override
	public boolean $less$eq(SpeedSortKey other) {
		return !$greater(other);
	}

	@Override
	public int compare(SpeedSortKey other) {
		if(highSpeed - other.getHighSpeed() != 0){
			return (int)(highSpeed - other.getHighSpeed());
		}else if (mediumSpeed - other.getMediumSpeed() != 0) {
			return (int)(mediumSpeed - other.getMediumSpeed());
		}else if (normalSpeed - other.getNormalSpeed() != 0) {
			return (int)(normalSpeed - other.getNormalSpeed());
		}else if (lowSpeed - other.getLowSpeed() != 0) {
			return (int)(lowSpeed - other.getLowSpeed());
		}
		return 0;
	}

	@Override
	public int compareTo(SpeedSortKey other) {
		if(highSpeed - other.getHighSpeed() != 0){
			return (int)(highSpeed - other.getHighSpeed());
		}else if (mediumSpeed - other.getMediumSpeed() != 0) {
			return (int)(mediumSpeed - other.getMediumSpeed());
		}else if (normalSpeed - other.getNormalSpeed() != 0) {
			return (int)(normalSpeed - other.getNormalSpeed());
		}else if (lowSpeed - other.getLowSpeed() != 0) {
			return (int)(lowSpeed - other.getLowSpeed());
		}
		return 0;
	}

	public long getLowSpeed() {
		return lowSpeed;
	}

	public void setLowSpeed(long lowSpeed) {
		this.lowSpeed = lowSpeed;
	}

	public long getNormalSpeed() {
		return normalSpeed;
	}

	public void setNormalSpeed(long normalSpeed) {
		this.normalSpeed = normalSpeed;
	}

	public long getMediumSpeed() {
		return mediumSpeed;
	}

	public void setMediumSpeed(long mediumSpeed) {
		this.mediumSpeed = mediumSpeed;
	}

	public long getHighSpeed() {
		return highSpeed;
	}

	public void setHighSpeed(long highSpeed) {
		this.highSpeed = highSpeed;
	}

	@Override
	public String toString() {
		return "SpeedSortKey [lowSpeed=" + lowSpeed + ", normalSpeed=" + normalSpeed + ", mediumSpeed=" + mediumSpeed + ", highSpeed=" + highSpeed + "]";
	}
}
