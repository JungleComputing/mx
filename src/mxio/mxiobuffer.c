#include "mxiobuffer.h"

void doGet
	(JNIEnv *env,
		  jarray dst, jint off,
		  jobject src, jint position, jint size) {

	jboolean isCopy = JNI_FALSE;
	void *dstbuf = (*env)->GetPrimitiveArrayCritical(env, dst, &isCopy);
	void *srcbuf = (*env)->GetDirectBufferAddress(env, src);

	memcpy(dstbuf + off , srcbuf + position, size);
	if(isCopy == JNI_FALSE) {
		(*env)->ReleasePrimitiveArrayCritical(env, dst, dstbuf, JNI_ABORT);
	} else {
		(*env)->ReleasePrimitiveArrayCritical(env, dst, dstbuf, 0);
	}
}

JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doGet___3CILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		  jcharArray dst, jint off,
		  jobject source, jint position, jint size) {
	doGet(env, dst, off, source, position, size);
}

JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doGet___3SILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		  jshortArray dst, jint off,
		  jobject source, jint position, jint size) {
	doGet(env, dst, off, source, position, size);
}

JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doGet___3IILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		  jintArray dst, jint off,
		  jobject source, jint position, jint size) {
	doGet(env, dst, off, source, position, size);
}


JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doGet___3JILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		  jlongArray dst, jint off,
		  jobject source, jint position, jint size) {
	doGet(env, dst, off, source, position, size);
}

JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doGet___3DILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		  jdoubleArray dst, jint off,
		  jobject source, jint position, jint size) {
	doGet(env, dst, off, source, position, size);
}

JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doGet___3FILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		  jfloatArray dst, jint off,
		  jobject source, jint position, jint size) {
	doGet(env, dst, off, source, position, size);
}

void doPut
	(JNIEnv *env,
		  jarray src, jint off,
		  jobject dst, jint position, jint size) {

	jboolean isCopy = JNI_FALSE;
	void *srcbuf = (*env)->GetPrimitiveArrayCritical(env, src, &isCopy);
	void *dstbuf = (*env)->GetDirectBufferAddress(env, dst);

	memcpy(dstbuf + position , srcbuf + off, size);
	(*env)->ReleasePrimitiveArrayCritical(env, src, srcbuf, JNI_ABORT);
}


JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doPut___3CILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		jcharArray src, jint off,
		jobject dst, jint position, jint size) {
	doPut(env, src, off, dst, position, size);
}


JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doPut___3SILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		jshortArray src, jint off,
		jobject dst, jint position, jint size) {
	doPut(env, src, off, dst, position, size);
}

JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doPut___3IILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		jintArray src, jint off,
		jobject dst, jint position, jint size) {
	doPut(env, src, off, dst, position, size);
}

JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doPut___3JILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		jlongArray src, jint off,
		jobject dst, jint position, jint size) {
	doPut(env, src, off, dst, position, size);
}

JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doPut___3DILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		jdoubleArray src, jint off,
		jobject dst, jint position, jint size) {
	doPut(env, src, off, dst, position, size);
}

JNIEXPORT void
JNICALL Java_mxio_MxIOBuffer_doPut___3FILjava_nio_ByteBuffer_2II
	(JNIEnv *env, jobject obj,
		jfloatArray src, jint off,
		jobject dst, jint position, jint size) {
	doPut(env, src, off, dst, position, size);
}
