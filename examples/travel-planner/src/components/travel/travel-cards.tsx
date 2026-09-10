import { Tabs } from "@base-ui/react/tabs";
import { ArrowUpRight, CalendarDays, MapPin, Plane, Utensils, Compass } from "lucide-react";

import type {
  FlightOption,
  ItineraryOption,
  PlaceOption,
  StayOption,
  TravelContent,
} from "../../travel-content";
import { Card, CardContent } from "../ui/card";
import { PhotoGallery, TravelImage } from "./photo-gallery";

function SourceLink({
  url,
  children,
}: {
  readonly url: string;
  readonly children: React.ReactNode;
}) {
  return (
    <a className="travel-source" href={url} target="_blank" rel="noopener noreferrer">
      {children}
      <ArrowUpRight size={15} aria-hidden="true" />
    </a>
  );
}

function sourceName(url: string) {
  return new URL(url).hostname.replace(/^www\./, "");
}

export function StayCard({ stay }: { readonly stay: StayOption }) {
  return (
    <Card className="stay-card">
      <PhotoGallery photos={stay.photos} name={stay.name} />
      <CardContent>
        <span className="travel-overline">STAY · {sourceName(stay.url)}</span>
        <h3>{stay.name}</h3>
        <div className="travel-location">
          <MapPin size={13} aria-hidden="true" />
          {stay.location}
        </div>
        {stay.highlights.length > 0 && (
          <ul className="travel-highlights">
            {stay.highlights.map((highlight, index) => (
              <li key={index}>{highlight}</li>
            ))}
          </ul>
        )}
        {stay.note && <p className="travel-note">{stay.note}</p>}
        <div className="travel-card-footer">
          <span className={stay.price ? "travel-price" : "travel-price-empty"}>
            {stay.price ?? "Check dates & price"}
          </span>
          <SourceLink url={stay.url}>View stay</SourceLink>
        </div>
      </CardContent>
    </Card>
  );
}

export function FlightCard({ flight }: { readonly flight: FlightOption }) {
  return (
    <Card className="flight-card">
      <CardContent>
        <div className="flight-heading">
          <span className="travel-overline">
            <Plane size={14} aria-hidden="true" />
            {flight.airline}
          </span>
          <span className={flight.price ? "travel-price" : "travel-price-empty"}>
            {flight.price ?? "Fare to check"}
          </span>
        </div>
        <div className="flight-route">
          <div>
            <strong>{flight.origin}</strong>
            <span>{flight.departure ?? "Departure to confirm"}</span>
          </div>
          <div className="flight-path">
            <span>{flight.duration ?? ""}</span>
            <div>
              <span />
              <Plane size={17} aria-hidden="true" />
              <span />
            </div>
            <span>{flight.stops ?? "Route to confirm"}</span>
          </div>
          <div>
            <strong>{flight.destination}</strong>
            <span>{flight.arrival ?? "Arrival to confirm"}</span>
          </div>
        </div>
        <div className="travel-card-footer">
          {flight.note && <p className="travel-note">{flight.note}</p>}
          <SourceLink url={flight.url}>View flight</SourceLink>
        </div>
      </CardContent>
    </Card>
  );
}

export function ItineraryCard({ itinerary }: { readonly itinerary: ItineraryOption }) {
  return (
    <Card className="itinerary-card">
      <CardContent>
        <span className="travel-overline">
          <CalendarDays size={14} aria-hidden="true" />
          YOUR DAYS, AT A GLANCE
        </span>
        <h3>{itinerary.title}</h3>
        <Tabs.Root defaultValue={0}>
          <Tabs.List className="itinerary-dates" aria-label={itinerary.title}>
            {itinerary.days.map((day, index) => (
              <Tabs.Tab key={index} value={index} className="itinerary-date">
                <span>DAY {String(index + 1).padStart(2, "0")}</span>
                <strong>
                  {day.date ? `${day.date.slice(5, 7)} / ${day.date.slice(8, 10)}` : "Flexible"}
                </strong>
              </Tabs.Tab>
            ))}
          </Tabs.List>
          {itinerary.days.map((day, index) => (
            <Tabs.Panel key={index} value={index} className="itinerary-day">
              <h4>{day.label}</h4>
              {day.date && <time dateTime={day.date}>{day.date}</time>}
              <ol className="day-timeline">
                {day.activities.map((activity, activityIndex) => (
                  <li key={activityIndex}>
                    <span className="timeline-dot" />
                    <div>
                      {activity.time && <span className="timeline-time">{activity.time}</span>}
                      <strong>{activity.title}</strong>
                      {activity.description && <p>{activity.description}</p>}
                      {activity.url && <SourceLink url={activity.url}>Details</SourceLink>}
                    </div>
                  </li>
                ))}
              </ol>
            </Tabs.Panel>
          ))}
        </Tabs.Root>
      </CardContent>
    </Card>
  );
}

export function PlaceCard({ place }: { readonly place: PlaceOption }) {
  const Icon = place.category === "restaurant" ? Utensils : Compass;

  return (
    <Card className="place-card">
      {place.photo && (
        <div className="place-photo">
          <TravelImage src={place.photo.url} alt={place.photo.caption} />
        </div>
      )}
      <CardContent>
        <span className="travel-overline">
          <Icon size={14} aria-hidden="true" />
          {place.category}
        </span>
        <h3>{place.name}</h3>
        <div className="travel-location">
          <MapPin size={13} aria-hidden="true" />
          {place.location}
        </div>
        <p className="travel-note">{place.description}</p>
        <div className="travel-card-footer">
          <SourceLink url={place.url}>Explore</SourceLink>
        </div>
      </CardContent>
    </Card>
  );
}

export function TravelCards({ content }: { readonly content: TravelContent }) {
  return (
    <section className="travel-content" aria-label={content.title}>
      <h2>{content.title}</h2>
      <div className="travel-grid">
        {content.items.map((item, index) => {
          switch (item.kind) {
            case "stay":
              return <StayCard key={index} stay={item} />;
            case "flight":
              return <FlightCard key={index} flight={item} />;
            case "itinerary":
              return <ItineraryCard key={index} itinerary={item} />;
            case "place":
              return <PlaceCard key={index} place={item} />;
          }
        })}
      </div>
    </section>
  );
}
