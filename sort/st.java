package com.ghgj.cn.sort;
/*
 * һ������  ��������������  û����������
 * 	�ӵ������¿�ʼ��ֳ  ÿ����ÿһ������  ��ֳһ��
 * 1	1	2	3	5	8	13
 * 24    22  21+20   23  22+21
 * 
 * ����֮��   ���ӵ��ܶ���
 * 
 * 	
 */
public class st {
	
	public static void main(String[] args) {
		int bsst = bsst(24);
		System.out.println(bsst);
	}
	//1.����һ����ͨ����  24   23+22
	//��������Ҫ����·�
	public static int bsst(int month){
		//2.�˳�����   month==1   month==2
		if(month==1||month==2){
			return 1;
		}else{
			return bsst(month-1)+bsst(month-2);
		}
		
	}
}
