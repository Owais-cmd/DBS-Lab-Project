// lib/priceFormatter.ts
export const formatPrice = (price: number): string => {
  return new Intl.NumberFormat('en-IN', {
    style: 'currency',
    currency: 'INR',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(price);
};

export const formatPriceSimple = (price: number): string => {
  return `₹${Math.round(price).toLocaleString('en-IN')}`;
};