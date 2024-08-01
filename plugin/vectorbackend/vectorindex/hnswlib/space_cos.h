#pragma once
#include <cmath>

#include "hnswlib.h"
#include "space_ip.h"

namespace hnswlib {

static float CosineDistance(const void *pVect1v, const void *pVect2v,
                            const size_t qty) {
  auto *pVect1 = (float *)pVect1v;
  auto *pVect2 = (float *)pVect2v;

  float dot = 0.0f;
  float norm1 = 0.0f;
  float norm2 = 0.0f;
  const float *pEnd = pVect1 + qty;
  while (pVect1 < pEnd) {
    dot += *pVect1 * *pVect2;
    norm1 += *pVect1 * *pVect1;
    norm2 += *pVect2 * *pVect2;
    pVect1++;
    pVect2++;
  }
  return 1.0f - dot / sqrtf(norm1 * norm2);
}

static float CosineDistanceOptimized(const void *pVect1v, const void *pVect2v,
                                     const size_t qty) {
  auto *pVect1 = (float *)pVect1v;
  auto *pVect2 = (float *)pVect2v;

  float norm1 = 0.0f;
  float norm2 = 0.0f;
  float dot = 0.0f;
  for (int i = 0; i < qty; ++i) {
    norm1 += pVect1[i] * pVect1[i];
    norm2 += pVect2[i] * pVect2[i];
    dot += pVect1[i] * pVect2[i];
  }

  norm1 = 1 / sqrtf(norm1);
  norm2 = 1 / sqrtf(norm2);

  for (int i = 0; i < qty; ++i) {
  }
}

#if defined(USE_AVX)
static float CosineDistanceSIMD4ExtAVX(const void *pVect1v, const void *pVect2v,
                                       const size_t qty) {
  auto *pVect1 = (float *)pVect1v;
  auto *pVect2 = (float *)pVect2v;

  __m256 sum256 = _mm256_setzero_ps();
  __m256 norm_v1_256 = _mm256_setzero_ps();
  __m256 norm_v2_256 = _mm256_setzero_ps();

  size_t qty16 = qty / 16;
  size_t qty4 = qty / 4;

  const float *pEnd1 = pVect1 + 16 * qty16;
  const float *pEnd2 = pVect1 + 4 * qty4;

  while (pVect1 < pEnd1) {
    //_mm_prefetch((char*)(pVect2 + 16), _MM_HINT_T0);
    __m256 v1 = _mm256_loadu_ps(pVect1);
    pVect1 += 8;
    __m256 v2 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;

    sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1, v2));
    norm_v1_256 = _mm256_add_ps(norm_v1_256, _mm256_mul_ps(v1, v1));
    norm_v2_256 = _mm256_add_ps(norm_v2_256, _mm256_mul_ps(v2, v2));

    v1 = _mm256_loadu_ps(pVect1);
    pVect1 += 8;
    v2 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;

    sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1, v2));
    norm_v1_256 = _mm256_add_ps(norm_v1_256, _mm256_mul_ps(v1, v1));
    norm_v2_256 = _mm256_add_ps(norm_v2_256, _mm256_mul_ps(v2, v2));
  }

  __m128 sum_prod = _mm_add_ps(_mm256_extractf128_ps(sum256, 0),
                               _mm256_extractf128_ps(sum256, 1));
  __m128 norm_v1 = _mm_add_ps(_mm256_extractf128_ps(norm_v1_256, 0),
                              _mm256_extractf128_ps(norm_v1_256, 1));
  __m128 norm_v2 = _mm_add_ps(_mm256_extractf128_ps(norm_v2_256, 0),
                              _mm256_extractf128_ps(norm_v2_256, 1));

  __m128 v1, v2;
  while (pVect1 < pEnd2) {
    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));
  }

  sum_prod = _mm_hadd_ps(sum_prod, sum_prod);
  norm_v1 = _mm_hadd_ps(norm_v1, norm_v1);
  norm_v2 = _mm_hadd_ps(norm_v2, norm_v2);

  float dot = _mm_cvtss_f32(_mm_hadd_ps(sum_prod, sum_prod));
  float norm_x = sqrtf(_mm_cvtss_f32(_mm_hadd_ps(norm_v1, norm_v1)));
  float norm_y = sqrtf(_mm_cvtss_f32(_mm_hadd_ps(norm_v2, norm_v2)));

  return 1.0f - dot / (norm_x * norm_y);
}

#endif

#if defined(USE_SSE)
static float CosineDistanceSIMD4ExtSSE(const void *pVect1v, const void *pVect2v,
                                       const size_t qty) {
  auto *pVect1 = (float *)pVect1v;
  auto *pVect2 = (float *)pVect2v;

  __m128 v1, v2;
  __m128 sum_prod = _mm_setzero_ps();
  __m128 norm_v1 = _mm_setzero_ps();
  __m128 norm_v2 = _mm_setzero_ps();

  size_t qty16 = qty / 16;
  size_t qty4 = qty / 4;

  const float *pEnd1 = pVect1 + 16 * qty16;
  const float *pEnd2 = pVect1 + 4 * qty4;

  while (pVect1 < pEnd1) {
    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));
  }

  while (pVect1 < pEnd2) {
    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));
  }

  sum_prod = _mm_hadd_ps(sum_prod, sum_prod);
  norm_v1 = _mm_hadd_ps(norm_v1, norm_v1);
  norm_v2 = _mm_hadd_ps(norm_v2, norm_v2);

  float dot = _mm_cvtss_f32(_mm_hadd_ps(sum_prod, sum_prod));
  float norm_x = sqrtf(_mm_cvtss_f32(_mm_hadd_ps(norm_v1, norm_v1)));
  float norm_y = sqrtf(_mm_cvtss_f32(_mm_hadd_ps(norm_v2, norm_v2)));

  return 1.0f - dot / (norm_x * norm_y);
}

#endif

#if defined(USE_AVX512)

inline float ExtractM512Sum(__m512 sum) {
  __m128 v1 = _mm512_extractf32x4_ps(sum, 0);
  __m128 v2 = _mm512_extractf32x4_ps(sum, 1);
  __m128 v3 = _mm512_extractf32x4_ps(sum, 2);
  __m128 v4 = _mm512_extractf32x4_ps(sum, 3);

  __m256 v12 =
      _mm256_add_ps(_mm256_castps128_ps256(v1), _mm256_castps128_ps256(v2));
  __m256 v34 =
      _mm256_add_ps(_mm256_castps128_ps256(v3), _mm256_castps128_ps256(v4));

  __m256 result = _mm256_add_ps(v12, v34);
  result = _mm256_hadd_ps(result, result);

  return _mm_cvtss_f32(_mm256_castps256_ps128(result));
}

static float CosineDistanceSIMD16ExtAVX512(const void *pVect1v,
                                           const void *pVect2v,
                                           const size_t qty) {
  auto *pVect1 = (float *)pVect1v;
  auto *pVect2 = (float *)pVect2v;

  __m512 sum_prod = _mm512_setzero_ps();
  __m512 norm_v1_512 = _mm512_setzero_ps();
  __m512 norm_v2_512 = _mm512_setzero_ps();

  size_t qty16 = qty / 16;

  const float *pEnd1 = pVect1 + 16 * qty16;

  while (pVect1 < pEnd1) {
    //_mm_prefetch((char*)(pVect2 + 16), _MM_HINT_T0);
    __m512 v1 = _mm512_loadu_ps(pVect1);
    pVect1 += 16;
    __m512 v2 = _mm512_loadu_ps(pVect2);
    pVect2 += 16;

    sum_prod = _mm512_add_ps(sum_prod, _mm512_mul_ps(v1, v2));
    norm_v1_512 = _mm512_add_ps(norm_v1_512, _mm512_mul_ps(v1, v1));
    norm_v2_512 = _mm512_add_ps(norm_v2_512, _mm512_mul_ps(v2, v2));
  }

  float dot = ExtractM512Sum(sum_prod);
  float norm_x = sqrtf(ExtractM512Sum(norm_v2_512));
  float norm_y = sqrtf(ExtractM512Sum(norm_v2_512));

  return 1.0f - dot / (norm_x * norm_y);
}

#endif

#if defined(USE_AVX)
static float CosineDistanceSIMD16ExtAVX(const void *pVect1v,
                                        const void *pVect2v, const size_t qty) {
  auto *pVect1 = (float *)pVect1v;
  auto *pVect2 = (float *)pVect2v;

  __m256 sum256 = _mm256_setzero_ps();
  __m256 norm_v1_256 = _mm256_setzero_ps();
  __m256 norm_v2_256 = _mm256_setzero_ps();

  size_t qty16 = qty / 16;

  const float *pEnd1 = pVect1 + 16 * qty16;

  while (pVect1 < pEnd1) {
    //_mm_prefetch((char*)(pVect2 + 16), _MM_HINT_T0);
    __m256 v1 = _mm256_loadu_ps(pVect1);
    pVect1 += 8;
    __m256 v2 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;

    sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1, v2));
    norm_v1_256 = _mm256_add_ps(norm_v1_256, _mm256_mul_ps(v1, v1));
    norm_v2_256 = _mm256_add_ps(norm_v2_256, _mm256_mul_ps(v2, v2));

    v1 = _mm256_loadu_ps(pVect1);
    pVect1 += 8;
    v2 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;

    sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1, v2));
    norm_v1_256 = _mm256_add_ps(norm_v1_256, _mm256_mul_ps(v1, v1));
    norm_v2_256 = _mm256_add_ps(norm_v2_256, _mm256_mul_ps(v2, v2));
  }

  for (int i = 0; i < 4; ++i) {
    sum256 = _mm256_hadd_ps(sum256, sum256);
    norm_v1_256 = _mm256_hadd_ps(norm_v1_256, norm_v1_256);
    norm_v2_256 = _mm256_hadd_ps(norm_v2_256, norm_v2_256);
  }
  float dot = _mm256_cvtss_f32(sum256);
  float norm_x = sqrtf(_mm256_cvtss_f32(norm_v1_256));
  float norm_y = sqrtf(_mm256_cvtss_f32(norm_v2_256));

  return 1.0f - dot / (norm_x * norm_y);
}

#endif

#if defined(USE_SSE)

static float CosineDistanceSIMD16ExtSSE(const void *pVect1v,
                                        const void *pVect2v, const size_t qty) {
  auto *pVect1 = (float *)pVect1v;
  auto *pVect2 = (float *)pVect2v;

  __m128 v1, v2;
  __m128 sum_prod = _mm_setzero_ps();
  __m128 norm_v1 = _mm_setzero_ps();
  __m128 norm_v2 = _mm_setzero_ps();

  size_t qty16 = qty / 16;

  const float *pEnd1 = pVect1 + 16 * qty16;

  while (pVect1 < pEnd1) {
    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));
  }

  sum_prod = _mm_hadd_ps(sum_prod, sum_prod);
  norm_v1 = _mm_hadd_ps(norm_v1, norm_v1);
  norm_v2 = _mm_hadd_ps(norm_v2, norm_v2);

  float dot = _mm_cvtss_f32(_mm_hadd_ps(sum_prod, sum_prod));
  float norm_x = sqrtf(_mm_cvtss_f32(_mm_hadd_ps(norm_v1, norm_v1)));
  float norm_y = sqrtf(_mm_cvtss_f32(_mm_hadd_ps(norm_v2, norm_v2)));

  return 1.0f - dot / (norm_x * norm_y);
}

#endif

#if defined(USE_SSE) || defined(USE_AVX) || defined(USE_AVX512)
static DISTFUNC<float> CosineDistanceSIMD16Ext = CosineDistanceSIMD16ExtSSE;
static DISTFUNC<float> CosineDistanceSIMD4Ext = CosineDistanceSIMD4ExtSSE;

static float CosineDistanceSIMD16ExtResiduals(const void *pVect1v,
                                              const void *pVect2v,
                                              const size_t qty) {
  auto *pVect1 = (float *)pVect1v;
  auto *pVect2 = (float *)pVect2v;

  __m128 v1, v2;
  __m128 sum_prod = _mm_setzero_ps();
  __m128 norm_v1 = _mm_setzero_ps();
  __m128 norm_v2 = _mm_setzero_ps();

  const float *pEnd1 = pVect1 + (qty >> 4 << 4);

  while (pVect1 < pEnd1) {
    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));
  }

  sum_prod = _mm_hadd_ps(sum_prod, sum_prod);
  sum_prod = _mm_hadd_ps(sum_prod, sum_prod);
  float dot = _mm_cvtss_f32(sum_prod);
  norm_v1 = _mm_hadd_ps(norm_v1, norm_v1);
  norm_v1 = _mm_hadd_ps(norm_v1, norm_v1);
  float norm1 = _mm_cvtss_f32(norm_v1);
  norm_v2 = _mm_hadd_ps(norm_v2, norm_v2);
  norm_v2 = _mm_hadd_ps(norm_v2, norm_v2);
  float norm2 = _mm_cvtss_f32(norm_v2);

  // calc tail
  const float *pEnd = pVect1 + qty;
  while (pVect1 < pEnd) {
    dot += *pVect1 * *pVect2;
    norm1 += *pVect1 * *pVect1;
    norm2 += *pVect2 * *pVect2;
    pVect1++;
    pVect2++;
  }

  return 1.0f - dot / sqrtf(norm1 * norm2);
}

static float CosineDistanceSIMD4ExtResiduals(const void *pVect1v,
                                             const void *pVect2v,
                                             const size_t qty) {
  auto *pVect1 = (float *)pVect1v;
  auto *pVect2 = (float *)pVect2v;

  __m128 v1, v2;
  __m128 sum_prod = _mm_setzero_ps();
  __m128 norm_v1 = _mm_setzero_ps();
  __m128 norm_v2 = _mm_setzero_ps();

  const float *pEnd1 = pVect1 + (qty >> 4 << 4);
  const float *pEnd2 = pVect1 + (qty >> 2 << 2);

  while (pVect1 < pEnd1) {
    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));

    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));
  }

  while (pVect1 < pEnd2) {
    v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
    norm_v1 = _mm_add_ps(norm_v1, _mm_mul_ps(v1, v1));
    norm_v2 = _mm_add_ps(norm_v2, _mm_mul_ps(v2, v2));
  }

  sum_prod = _mm_hadd_ps(sum_prod, sum_prod);
  sum_prod = _mm_hadd_ps(sum_prod, sum_prod);
  float dot = _mm_cvtss_f32(sum_prod);
  norm_v1 = _mm_hadd_ps(norm_v1, norm_v1);
  norm_v1 = _mm_hadd_ps(norm_v1, norm_v1);
  float norm1 = _mm_cvtss_f32(norm_v1);
  norm_v2 = _mm_hadd_ps(norm_v2, norm_v2);
  norm_v2 = _mm_hadd_ps(norm_v2, norm_v2);
  float norm2 = _mm_cvtss_f32(norm_v2);

  // calc tail
  const float *pEnd = pVect1 + qty;
  while (pVect1 < pEnd) {
    dot += *pVect1 * *pVect2;
    norm1 += *pVect1 * *pVect1;
    norm2 += *pVect2 * *pVect2;
    pVect1++;
    pVect2++;
  }

  return 1.0f - dot / sqrtf(norm1 * norm2);
}
#endif

class CosineSpace : public SpaceInterface<float> {
  DISTFUNC<float> fstdistfunc_;
  size_t data_size_;
  size_t dim_;

 public:
  explicit CosineSpace(size_t dim) {
    fstdistfunc_ = CosineDistance;
#if defined(USE_AVX) || defined(USE_SSE) || defined(USE_AVX512)
#if defined(USE_AVX512)
    if (AVX512Capable()) {
      CosineDistanceSIMD16Ext = CosineDistanceSIMD16ExtAVX512;
    } else if (AVXCapable()) {
      CosineDistanceSIMD16Ext = CosineDistanceSIMD16ExtAVX;
    }
#elif defined(USE_AVX)
    if (AVXCapable()) {
      CosineDistanceSIMD16Ext = CosineDistanceSIMD16ExtAVX;
    }
#endif
#if defined(USE_AVX)
    if (AVXCapable()) {
      CosineDistanceSIMD4Ext = CosineDistanceSIMD4ExtAVX;
    }
#endif

    if (dim % 16 == 0)
      fstdistfunc_ = CosineDistanceSIMD16Ext;
    else if (dim % 4 == 0)
      fstdistfunc_ = CosineDistanceSIMD4Ext;
    else if (dim > 16)
      fstdistfunc_ = CosineDistanceSIMD16ExtResiduals;
    else if (dim > 4)
      fstdistfunc_ = CosineDistanceSIMD4ExtResiduals;
#endif
    dim_ = dim;
    data_size_ = dim * sizeof(float);
  }

  size_t get_data_size() override { return data_size_; }

  DISTFUNC<float> get_dist_func() override { return fstdistfunc_; }

  size_t get_dist_func_param() override { return dim_; }

  ~CosineSpace() override = default;
};

}  // namespace hnswlib
