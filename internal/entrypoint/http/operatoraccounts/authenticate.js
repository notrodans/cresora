(() => {
	const item = document.querySelector("[data-expires]");
	if (!item) return;
	const until = Number(item.dataset.expires);
	const label = item.querySelector("strong");
	let refreshed = false;
	const tick = () => {
		const left = Math.max(0, until - Date.now());
		label.textContent = `${Math.floor(left / 60000)}:${String(Math.floor(left / 1000) % 60).padStart(2, "0")}`;
		if (left === 0 && !refreshed) {
			refreshed = true;
			label.textContent = "обновление";
			item.closest(".qr-method").querySelector("form").requestSubmit();
		}
	};
	tick();
	window.setInterval(tick, 1000);
})();
