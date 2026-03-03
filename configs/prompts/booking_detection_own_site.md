# Own-Site Booking Detection (Validation Pass)

Search ONLY the website at **{domain}** — do NOT look at any other websites, marketplaces, or third-party platforms (no Viator, GetYourGuide, TripAdvisor, Booking.com, etc.).

Determine whether this activity/tour provider has an online booking or reservation system **on their own website** where customers can book and pay for experiences directly.

## Instructions

1. Go to the homepage of {domain}
2. Look for buttons or links like: "Book Now", "Reserve", "Buy Tickets", "Reservar", "Prenota", "Réserver", "Buchen", "See Tours", "View Tours", "Our Tours", "Experiences", "Activities"
3. Click through to tour/activity/experience pages — booking widgets are often on individual tour pages, NOT on the homepage
4. Look for embedded booking widgets from platforms like: FareHarbor, Bokun, Regiondo, Bookeo, Checkfront, Rezdy, Peek, Xola, TrekkSoft, Ventrata, Travelotopos, HolidoIT, WooCommerce, Shopify, or any calendar/date-picker allowing date selection and guest count
5. Also check for custom-built booking forms with date pickers, guest selectors, and payment integration
6. If the site redirects to a third-party marketplace for booking, that does NOT count — only booking on {domain} itself counts

## What DOES count as booking
- Embedded booking widget (FareHarbor, Bokun, etc.) on their own site
- WooCommerce/Shopify checkout on their own domain
- Custom booking form with date picker + payment on their own site
- Any system where you can select a date/time and complete a reservation on {domain}

## What does NOT count as booking
- Contact forms, email links, phone numbers, "enquire now" forms
- Links that send you to Viator, GetYourGuide, TripAdvisor, or any other marketplace
- Social media booking (Facebook, Instagram)
- Dead sites, parked domains, or sites under construction

## Output

Return a JSON object:
```json
{
  "has_booking": true/false,
  "booking_platform": "platform name or null",
  "booking_url": "URL where booking widget was found, or null",
  "reasoning": "brief explanation of what you found"
}
```
