(() => {
	const account = document.querySelector("#account_id");
	if (!account) return;
	const targets = document.querySelectorAll("[data-account-id]");
	const filter = () =>
		targets.forEach((target) => {
			const hidden =
				!!account.value && target.dataset.accountId !== account.value;
			target.hidden = hidden;
			const input = target.querySelector("input");
			if (input) input.disabled = hidden;
		});
	account.addEventListener("change", filter);
	filter();
})();
