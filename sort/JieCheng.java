package com.ghgj.cn.sort;

public class JieCheng {
	public static void main(String[] args) {
		int jc = jc(5);
		System.out.println(jc);
	}
	
	//1.����һ����ͨ����  ������׳˵�   n!  n=5  5*4!  4*3��   3*2��   2*1!  1
	//��������Ҫ���ĳһ��ֵ
	public static int jc(int n){
		//2.���˳�����  n=1
		if(n==1){//1!
			return 1;//����ֵ  �������  ������ʱ����Ľ׳�  
		}else{//��Ҫ�ݹ�
			//3.�ҹ���
			return n*jc(n-1);
		}
		/*jc(n)  n!
		 * jc(n-1)  n-1!
		 * �������ù���
		 * jc(5)
		 * ���̣�
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
