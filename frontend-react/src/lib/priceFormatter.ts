// lib/priceFormatter.ts
export const formatPrice = (priceInUSD: number): string => {
  const priceInINR = priceInUSD * 83;
  return new Intl.NumberFormat('en-IN', {
    style: 'currency',
    currency: 'INR',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(priceInINR);
};

export const formatPriceSimple = (priceInUSD: number): string => {
  const priceInINR = priceInUSD * 83;
  return `₹${Math.round(priceInINR).toLocaleString('en-IN')}`;
};
