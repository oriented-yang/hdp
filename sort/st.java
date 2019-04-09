package com.ghgj.cn.sort;
/*
 * 一对兔子  假设整个过程中  没有兔子死亡
 * 	从第三个月开始繁殖  每个月每一对兔子  繁殖一对
 * 1	1	2	3	5	8	13
 * 24    22  21+20   23  22+21
 * 
 * 两年之后   兔子的总对数
 * 
 * 	
 */
public class st {
	
	public static void main(String[] args) {
		int bsst = bsst(24);
		System.out.println(bsst);
	}
	//1.定义一个普通方法  24   23+22
	//参数：需要求的月份
	public static int bsst(int month){
		//2.退出条件   month==1   month==2
		if(month==1||month==2){
			return 1;
		}else{
			return bsst(month-1)+bsst(month-2);
		}
		
	}
}
