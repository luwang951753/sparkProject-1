package com.ish.sparkproject.spark.session;


import scala.math.Ordered;

/**
 * categoryId品类的二次排序的key
 * 实现ordered接口要求的几个方法
 */
public class CategorySortKey implements Ordered<CategorySortKey>, java.io.Serializable {

	private long clickCount;
	private long orderCount;
	private long payCount;

	public CategorySortKey(long clickCount, long orderCount, long payCount) {
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.payCount = payCount;
	}

	public CategorySortKey() {
	}

	public long getClickCount() {
		return clickCount;
	}

	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}

	public long getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}

	public long getPayCount() {
		return payCount;
	}

	public void setPayCount(long payCount) {
		this.payCount = payCount;
	}


	@Override
	public int compare(CategorySortKey other) {
		if(clickCount - other.getClickCount() != 0) {
			return (int) (clickCount - other.getClickCount());
		} else if(orderCount - other.getOrderCount() != 0) {
			return (int) (orderCount - other.getOrderCount());
		} else if(payCount - other.getPayCount() != 0) {
			return (int) (payCount - other.getPayCount());
		}
		return 0;
	}

	@Override
	public int compareTo(CategorySortKey other) {
		if(clickCount - other.getClickCount() != 0) {
			return (int) (clickCount - other.getClickCount());
		} else if(orderCount - other.getOrderCount() != 0) {
			return (int) (orderCount - other.getOrderCount());
		} else if(payCount - other.getPayCount() != 0) {
			return (int) (payCount - other.getPayCount());
		}
		return 0;
	}

	@Override
	public boolean $less(CategorySortKey other) {
		if (clickCount < other.getClickCount()){
			return true;
		}else if (clickCount == other.getClickCount()
				&& orderCount < other.getOrderCount()){
			return true;
		}else if (clickCount == other.getClickCount()
				&& orderCount == other.getOrderCount()
				&& payCount < other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater(CategorySortKey other) {
		if (clickCount > other.getClickCount()){
			return true;
		}else if (clickCount == other.getClickCount()
				&& orderCount > other.getOrderCount()){
			return true;
		}else if (clickCount == other.getClickCount()
				&& orderCount == other.getOrderCount()
				&& payCount > other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(CategorySortKey other) {
		if ($less(other)){
			return true;
		}else if (clickCount == other.getClickCount()
				&& orderCount == other.getOrderCount()
				&& payCount == other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(CategorySortKey other) {
		if ($greater(other)){
			return true;
		}else if (clickCount == other.getClickCount()
				&& orderCount == other.getOrderCount()
				&& payCount == other.getPayCount()){
			return true;
		}
		return false;
	}

}
