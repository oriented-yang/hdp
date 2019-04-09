package com.ghgj.cn.sort;

public class CountSort {
	

	
	public static void main(String[] args) {
		int[] arr={2,3,4,6,23,1,4,2,4,6};
		count(arr);
	}
	
	//参数   原始数组
	public static void count(int[] arr){
		//创建一个新数组   长度  = 原始数组的最大值+1
		//求原始数组的最大值
		int max=arr[0];
		for(int a:arr){
			if(max<a){
				max=a;
			}
		}
		
		//创建新的数组了   最大值-最小值+1
		int[] newarr=new int[max+1];
		//循环遍历原始数组   将原始数组的值  放在新数组的下标中
		for(int a:arr){
			newarr[a]++;
		}
		//已经拍完序
		//循环遍历输出
		for(int i=0;i<newarr.length;i++){
			//循环遍历出现的次数
			/*if(newarr[i]>0){*/
			for(int j=0;j<newarr[i];j++){
				System.out.print(i+"\t");
			}
				
			//}
		}
		
	}
}
