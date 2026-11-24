<script lang="ts">
	import { Input } from '$lib/components/ui/input/index.js';
	import { Label } from '$lib/components/ui/label/index.js';

	let {
		id,
		value = $bindable(''),
		disabled = false
	}: { id: string; value: string; disabled?: boolean } = $props();
</script>

<div class="space-y-2">
	<Label for={id} class="text-sm font-medium">每日下载上限（条）</Label>
	<Input
		{id}
		type="text"
		inputmode="numeric"
		placeholder="留空不限"
		bind:value
		{disabled}
		aria-describedby={`${id}-help`}
	/>
	<p id={`${id}-help`} class="text-muted-foreground text-xs">
		同一投稿的多个分 P 计一条。达到上限后，剩余投稿在次日继续下载。留空或填 0 表示不限。
	</p>
</div>
