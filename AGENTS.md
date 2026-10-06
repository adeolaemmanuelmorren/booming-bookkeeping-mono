# Repository instructions

## Code style

Write extremely easy to consume code. Optimize for how easy the code is to read. Make the code skimmable. Avoid cleverness. Use early returns.

## Repository and deployment pattern

Use this root repository as the monorepo by default. Keep related Workers and Next.js apps here so changes to shared code can be reviewed together. Create a separate repository only when a tool requires it or the project needs independent ownership or releases.

- The proxy Worker belongs to this monorepo at `cloudflare-workers/reverse-proxy/`. Run its Git commands from the root repository.
- For Cloudflare Git builds, set each Worker's root directory to its project folder and configure build watch paths for that folder and any shared dependencies. Setting the root directory alone does not prevent unrelated builds.
- For the proxy, use root directory `cloudflare-workers/reverse-proxy` and include watch paths `cloudflare-workers/reverse-proxy/*` and `cloudflare-workers/source-gateways/*`. The proxy imports the shared Stripe gateway from the latter folder. Keep these paths current when adding shared dependencies.
- For Next.js on Vercel, configure skipping unaffected projects or an Ignored Build Step that includes shared dependencies.
- Keep ChatGPT Sites on their existing Sites save-and-deploy workflow. A push to this monorepo does not itself publish a Site.

## Independent nested repositories

Keep projects that need their own GitHub repository, such as Dataform, in a nested folder ignored by this root repository.

- Add the nested folder to the root `.gitignore`, for example `/dataform/`.
- Keep the nested project's own Git history and GitHub remote.
- Run Git commands from the repository being changed. Commit and push root changes from the root; commit and push Dataform changes from `dataform/`.
- Connect Google Dataform directly to the Dataform GitHub repository, with its configuration at that repository's root.
- Do not add the nested repository as a submodule or track its files in the root repository.
- On a fresh checkout, clone each independent repository into its expected folder separately.
- If the root already tracks a nested folder, check its tracked files and preserve local work before changing the setup. Adding an ignore rule alone does not stop tracking existing files.
