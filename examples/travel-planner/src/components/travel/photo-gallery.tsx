import { Dialog } from "@base-ui/react/dialog";
import { ChevronLeft, ChevronRight, Expand, ImageOff, X } from "lucide-react";
import { useState } from "react";

import { safeTravelUrl, type TravelPhoto } from "../../travel-content";
import { Button } from "../ui/button";

/** Reserve image space, hide broken-image chrome, and never send the conversation as a referrer. */
export function TravelImage({
  src,
  alt,
  contain = false,
}: {
  readonly src: string;
  readonly alt: string;
  readonly contain?: boolean;
}) {
  const [failed, setFailed] = useState(false);
  const url = safeTravelUrl(src);

  return failed || url === undefined ? (
    <span className="photo-unavailable">
      <ImageOff size={22} aria-hidden="true" />
      <span>Photo unavailable</span>
    </span>
  ) : (
    <img
      src={url}
      alt={alt}
      loading="lazy"
      decoding="async"
      referrerPolicy="no-referrer"
      className={contain ? "photo-contain" : "photo-cover"}
      onError={() => setFailed(true)}
    />
  );
}

export function PhotoGallery({
  photos,
  name,
}: {
  readonly photos: readonly TravelPhoto[];
  readonly name: string;
}) {
  const [index, setIndex] = useState(0);
  const current = photos[index] ?? photos[0];

  const move = (step: number) =>
    setIndex((value) => (value + step + photos.length) % photos.length);

  if (!current)
    return (
      <div className="photo-empty">
        <ImageOff size={22} aria-hidden="true" />
        <span>See photos on the listing</span>
      </div>
    );

  return (
    <Dialog.Root>
      <div className="property-gallery">
        <Dialog.Trigger className="gallery-open" aria-label={`View photos of ${name}`}>
          <TravelImage key={current.url} src={current.url} alt={current.caption} />
          <span className="gallery-expand">
            <Expand size={14} aria-hidden="true" />
          </span>
        </Dialog.Trigger>
        <span className="photo-count" aria-live="polite">
          {index + 1} / {photos.length}
        </span>
        {photos.length > 1 && (
          <div className="gallery-arrows">
            <Button
              variant="outline"
              size="icon"
              aria-label={`Previous photo of ${name}`}
              onClick={() => move(-1)}
            >
              <ChevronLeft size={17} />
            </Button>
            <Button
              variant="outline"
              size="icon"
              aria-label={`Next photo of ${name}`}
              onClick={() => move(1)}
            >
              <ChevronRight size={17} />
            </Button>
          </div>
        )}
      </div>
      <Dialog.Portal>
        <Dialog.Backdrop className="gallery-backdrop" />
        <Dialog.Popup
          className="gallery-dialog"
          onKeyDownCapture={(event) => {
            if (photos.length > 1 && (event.key === "ArrowLeft" || event.key === "ArrowRight")) {
              event.preventDefault();
              move(event.key === "ArrowLeft" ? -1 : 1);
            }
          }}
        >
          <header>
            <Dialog.Title>{name}</Dialog.Title>
            <Dialog.Close render={<Button variant="ghost" size="icon" />} aria-label="Close photos">
              <X size={20} />
            </Dialog.Close>
          </header>
          <div className="gallery-large">
            <TravelImage key={current.url} src={current.url} alt={current.caption} contain />
          </div>
          <footer>
            <Dialog.Description>{current.caption}</Dialog.Description>
            {photos.length > 1 && (
              <div className="gallery-navigation">
                <Button
                  variant="outline"
                  size="icon"
                  aria-label="Previous photo"
                  onClick={() => move(-1)}
                >
                  <ChevronLeft size={18} />
                </Button>
                <span aria-live="polite">
                  {index + 1} / {photos.length}
                </span>
                <Button
                  variant="outline"
                  size="icon"
                  aria-label="Next photo"
                  onClick={() => move(1)}
                >
                  <ChevronRight size={18} />
                </Button>
              </div>
            )}
          </footer>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
