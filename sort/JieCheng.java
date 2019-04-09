package com.ghgj.cn.sort;

public class JieCheng {
	public static void main(String[] args) {
		int jc = jc(5);
		System.out.println(jc);
	}
	
	//1.定义一个普通方法  用于求阶乘的   n!  n=5  5*4!  4*3！   3*2！   2*1!  1
	//参数：索要求的某一个值
	public static int jc(int n){
		//2.找退出条件  n=1
		if(n==1){//1!
			return 1;//返回值  代表的是  跳出的时候求的阶乘  
		}else{//需要递归
			//3.找规律
			return n*jc(n-1);
		}
		/*jc(n)  n!
		 * jc(n-1)  n-1!
		 * 方法调用过程
		 * jc(5)
		 * 过程：
		 * jc(5)   5! ===120  
		 * 5*jc(4)====24
		 * 	4*jc(3)======6
		 * 		3*jc(2)====2
		 * 			2*jc(1)===1	
		 * 			
		 * 
		 * 
		 */
	}
}
